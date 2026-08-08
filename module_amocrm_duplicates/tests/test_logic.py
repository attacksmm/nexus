from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


def load_router():
    try:
        import aiosqlite  # noqa: F401
        import fastapi  # noqa: F401
        import httpx  # noqa: F401
    except ModuleNotFoundError:
        aiosqlite = types.ModuleType("aiosqlite")

        class Cursor:
            def __init__(self, cursor):
                self.cursor = cursor
                self.rowcount = cursor.rowcount
                self.lastrowid = cursor.lastrowid

            async def fetchone(self):
                return self.cursor.fetchone()

            async def fetchall(self):
                return self.cursor.fetchall()

            def __aiter__(self):
                return self

            async def __anext__(self):
                row = self.cursor.fetchone()
                if row is None:
                    raise StopAsyncIteration
                return row

        class Connection:
            def __init__(self, path):
                self.connection = sqlite3.connect(path)

            @property
            def row_factory(self):
                return self.connection.row_factory

            @row_factory.setter
            def row_factory(self, value):
                self.connection.row_factory = value

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.connection.close()

            async def execute(self, sql, params=()):
                return Cursor(self.connection.execute(sql, params))

            async def executescript(self, sql):
                self.connection.executescript(sql)

            async def commit(self):
                self.connection.commit()

        aiosqlite.connect = Connection
        aiosqlite.Row = sqlite3.Row
        sys.modules["aiosqlite"] = aiosqlite

        class APIRouter:
            def get(self, *_args, **_kwargs):
                return lambda fn: fn

            post = get
            put = get

        class HTTPException(Exception):
            def __init__(self, status_code, detail):
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        fastapi = types.ModuleType("fastapi")
        fastapi.APIRouter = APIRouter
        fastapi.HTTPException = HTTPException
        fastapi.Request = object
        sys.modules["fastapi"] = fastapi

        httpx = types.ModuleType("httpx")
        httpx.AsyncClient = object
        sys.modules["httpx"] = httpx

        orchestrator = types.ModuleType("orchestrator")
        auth = types.ModuleType("orchestrator.auth")
        auth.can_access_module = lambda *_args: True

        async def verify_token_from_request(_request):
            return {"id": 1}

        auth.verify_token_from_request = verify_token_from_request
        sys.modules["orchestrator"] = orchestrator
        sys.modules["orchestrator.auth"] = auth

    path = Path(__file__).resolve().parents[1] / "router.py"
    spec = importlib.util.spec_from_file_location("amocrm_duplicates_router", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


r = load_router()


PHONE = {
    "id": "phone",
    "entity": "contacts",
    "field_id": "",
    "field_code": "PHONE",
    "field_name": "Телефон",
}
EMAIL = {
    "id": "email",
    "entity": "contacts",
    "field_id": "",
    "field_code": "EMAIL",
    "field_name": "Email",
}


def contact(phone="", email=""):
    fields = []
    if phone:
        fields.append({"field_code": "PHONE", "field_name": "Телефон", "values": [{"value": phone}]})
    if email:
        fields.append({"field_code": "EMAIL", "field_name": "Email", "values": [{"value": email}]})
    return {"id": 10, "custom_fields_values": fields}


class ConfigTests(unittest.TestCase):
    def test_default_config_is_disabled_and_uses_contact_phone(self):
        config = r._clean_config(r.DEFAULT_CONFIG)
        self.assertFalse(config["enabled"])
        self.assertFalse(config["copy_responsible_from_latest_duplicate"])
        self.assertFalse(config["ai"]["openrouter_summary_enabled"])
        self.assertEqual(config["search"]["groups"][0]["conditions"][0]["field_code"], "PHONE")
        self.assertEqual(config["base_tags"], ["Дубль?"])

    def test_enabled_config_requires_search_group_and_limited_scope(self):
        raw = dict(r.DEFAULT_CONFIG)
        raw["enabled"] = True
        raw["search"] = {"operator": "OR", "groups": []}
        with self.assertRaisesRegex(ValueError, "группу поиска"):
            r._clean_config(raw)

        raw["search"] = r.DEFAULT_CONFIG["search"]
        raw["source_scope"] = {"all": False, "statuses": []}
        with self.assertRaisesRegex(ValueError, "статус"):
            r._clean_config(raw)

    def test_duplicate_condition_ids_are_made_unique(self):
        raw = dict(r.DEFAULT_CONFIG)
        raw["search"] = {
            "operator": "AND",
            "groups": [
                {"name": "a", "operator": "AND", "conditions": [dict(PHONE)]},
                {"name": "b", "operator": "OR", "conditions": [dict(PHONE)]},
            ],
        }
        config = r._clean_config(raw)
        ids = [condition["id"] for group in config["search"]["groups"] for condition in group["conditions"]]
        self.assertEqual(len(ids), len(set(ids)))


class PanelSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")

    def test_default_work_rule_excludes_unsorted_statuses(self):
        self.assertIn("Number(s.type)!==1&&!['142','143'].includes(String(s.id))", self.source)

    def test_state_checkbox_updates_do_not_rebuild_open_rule(self):
        status_handler = self.source.split("function toggleRuleStatus", 1)[1].split("function toggleRulePipeline", 1)[0]
        pipeline_handler = self.source.split("function toggleRulePipeline", 1)[1].split("function refreshRuleCounts", 1)[0]
        self.assertIn("refreshRuleCounts(i)", status_handler)
        self.assertIn("refreshRuleCounts(i)", pipeline_handler)
        self.assertNotIn("renderStates()", status_handler)
        self.assertNotIn("renderStates()", pipeline_handler)

    def test_ai_tab_has_summary_switch_and_today_backfill(self):
        self.assertIn('data-view="ai">AI</button>', self.source)
        self.assertIn('id="aiSummaryEnabled"', self.source)
        self.assertIn("/ai/backfill-today", self.source)


class ParsingTests(unittest.TestCase):
    def test_platform_id_is_read_only_from_utm_term(self):
        lead = {
            "custom_fields_values": [
                {"field_name": "utm_term", "values": [{"value": "vk_884568514_sale"}]},
                {"field_code": "UTM_TERM", "values": [{"value": "repeat 884568514"}]},
                {"field_name": "other", "values": [{"value": "71045594"}]},
            ]
        }
        self.assertEqual(r._platform_ids_from_utm(lead), ["884568514"])

    def test_nested_and_flat_add_webhooks(self):
        nested = {"leads": {"add": [{"id": "123", "pipeline_id": "10", "status_id": "20"}]}}
        flat = {
            "leads[add][0][id]": "123",
            "leads[add][0][pipeline_id]": "10",
            "leads[add][0][status_id]": "20",
        }
        self.assertEqual(r._lead_add_events(nested), r._lead_add_events(flat))

    def test_non_add_webhook_is_ignored(self):
        self.assertEqual(r._lead_add_events({"leads": {"status": [{"id": "123"}]}}), [])

    def test_lead_added_events_are_normalized_and_deduplicated(self):
        body = {
            "_embedded": {
                "events": [
                    {"id": "event-1", "type": "lead_added", "entity_id": 123, "created_at": 100},
                    {"id": "event-2", "type": "lead_added", "entity_id": 123, "created_at": 101},
                    {"id": "event-3", "type": "lead_updated", "entity_id": 456, "created_at": 102},
                ]
            }
        }
        self.assertEqual(
            r._lead_added_event_rows(body),
            [{"id": "123", "pipeline_id": "", "status_id": "", "event_id": "event-2", "created_at": "101"}],
        )


class MatchTests(unittest.TestCase):
    def test_phone_and_email_normalization(self):
        self.assertTrue(r._values_overlap(["8 (999) 123-45-67"], ["+7 999 123 45 67"], PHONE))
        self.assertTrue(r._values_overlap([" User@Example.COM "], ["user@example.com"], EMAIL))
        self.assertFalse(r._values_overlap(["79991234567"], ["79997654321"], PHONE))

    def test_group_and_top_level_logic(self):
        source = {"phone": ["79991234567"], "email": ["a@example.com"]}
        candidate_contacts = [contact("+7 999 123-45-67", "wrong@example.com")]
        both = {"operator": "OR", "groups": [{"name": "both", "operator": "AND", "conditions": [PHONE, EMAIL]}]}
        any_one = {"operator": "OR", "groups": [{"name": "any", "operator": "OR", "conditions": [PHONE, EMAIL]}]}
        self.assertFalse(r._expression_matches(source, {}, candidate_contacts, both)[0])
        matched, fields = r._expression_matches(source, {}, candidate_contacts, any_one)
        self.assertTrue(matched)
        self.assertEqual(fields[0]["field"], "Телефон")

    def test_expression_readiness_respects_and_or(self):
        search_and = {"operator": "OR", "groups": [{"operator": "AND", "conditions": [PHONE, EMAIL]}]}
        search_or = {"operator": "OR", "groups": [{"operator": "OR", "conditions": [PHONE, EMAIL]}]}
        source = {"phone": ["79991234567"], "email": []}
        self.assertFalse(r._expression_ready(source, search_and))
        self.assertTrue(r._expression_ready(source, search_or))


class AiSummaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        r._module_dir = root / "amocrm-duplicates"
        db_path = root / "openrouter" / "data" / "openrouter.db"
        db_path.parent.mkdir(parents=True)
        db = sqlite3.connect(db_path)
        db.execute("CREATE TABLE users(platform_id TEXT PRIMARY KEY, summary TEXT NOT NULL DEFAULT '')")
        db.execute("INSERT INTO users(platform_id,summary) VALUES(?,?)", ("884568514", "**Коротко:** готов купить"))
        db.commit()
        db.close()
        self.config = {"ai": {"openrouter_summary_enabled": True}, "request_timeout": 15}
        self.lead = {
            "id": 123,
            "pipeline_id": 1,
            "status_id": 2,
            "responsible_user_id": 3,
            "custom_fields_values": [
                {"field_name": "utm_term", "values": [{"value": "884568514"}]}
            ],
        }

    def tearDown(self):
        self.temp.cleanup()

    async def test_creates_only_common_note_even_for_unsorted_lead(self):
        calls = []

        async def amo(method, path, _config, payload=None):
            calls.append((method, path, payload))
            if method == "GET":
                return {"_embedded": {"notes": []}}, "", 200
            return {}, "", 200

        with patch.object(r, "_amo_request", side_effect=amo):
            result = await r._apply_ai_summary(123, self.lead, self.config)

        self.assertEqual(result["state"], "created")
        self.assertEqual([call[0] for call in calls], ["GET", "POST"])
        self.assertNotIn("PATCH", [call[0] for call in calls])
        self.assertEqual(calls[1][1], "/api/v4/leads/123/notes")
        self.assertEqual(
            calls[1][2][0]["params"]["text"],
            "Общение с ИИ:\n\nКоротко: готов купить",
        )
        self.assertNotIn("**", calls[1][2][0]["params"]["text"])

    async def test_existing_ai_note_is_not_created_twice(self):
        body = {"_embedded": {"notes": [{"params": {"text": r.AI_NOTE_TITLE + "\n\nСводка"}}]}}
        request = AsyncMock(return_value=(body, "", 200))
        with patch.object(r, "_amo_request", request):
            result = await r._apply_ai_summary(123, self.lead, self.config)
        self.assertEqual(result["state"], "already_added")
        request.assert_awaited_once()

    async def test_legacy_ai_note_is_not_created_twice(self):
        body = {"_embedded": {"notes": [{"params": {"text": "Краткая сводка диалога\n\nСводка"}}]}}
        request = AsyncMock(return_value=(body, "", 200))
        with patch.object(r, "_amo_request", request):
            result = await r._apply_ai_summary(123, self.lead, self.config)
        self.assertEqual(result["state"], "already_added")
        request.assert_awaited_once()

    async def test_dry_run_does_not_create_note(self):
        request = AsyncMock(return_value=({"_embedded": {"notes": []}}, "", 200))
        with patch.object(r, "_amo_request", request):
            result = await r._apply_ai_summary(123, self.lead, self.config, dry_run=True)
        self.assertEqual(result["state"], "ready")
        request.assert_awaited_once()


class OutcomeTests(unittest.TestCase):
    def test_all_matching_state_tags_are_combined(self):
        config = {
            "base_tags": ["Дубль?"],
            "state_rules": [
                {"enabled": True, "responsible": "any", "statuses": [{"pipeline_id": "1", "status_id": "142"}], "tags": ["Успешно реализованное"]},
                {"enabled": True, "responsible": "assigned", "statuses": [{"pipeline_id": "2", "status_id": "20"}], "tags": ["В работе"]},
            ],
        }
        duplicates = [
            {"pipeline_id": "1", "status_id": "142", "responsible_user_id": 0},
            {"pipeline_id": "2", "status_id": "20", "responsible_user_id": 55},
        ]
        self.assertEqual(r._state_tags(duplicates, config), ["Дубль?", "Успешно реализованное", "В работе"])

    def test_working_tag_requires_responsible(self):
        config = {"base_tags": ["Дубль?"], "state_rules": [{"enabled": True, "responsible": "assigned", "statuses": [{"pipeline_id": "2", "status_id": "20"}], "tags": ["В работе"]}]}
        self.assertEqual(r._state_tags([{"pipeline_id": "2", "status_id": "20", "responsible_user_id": 0}], config), ["Дубль?"])

    def test_closed_lost_duplicate_adds_its_configured_tag(self):
        config = {"base_tags": ["Дубль"], "state_rules": [{"enabled": True, "responsible": "any", "statuses": [{"pipeline_id": "1", "status_id": "143"}], "tags": ["Закрыто и не реализовано"]}]}
        duplicate = {"pipeline_id": "1", "status_id": "143", "responsible_user_id": 7}
        self.assertEqual(r._state_tags([duplicate], config), ["Дубль", "Закрыто и не реализовано"])

    def test_source_scope_and_note_contain_every_duplicate(self):
        config = {"source_scope": {"all": False, "statuses": [{"pipeline_id": "1", "status_id": "10"}]}}
        self.assertTrue(r._scope_allows({"pipeline_id": 1, "status_id": 10}, config))
        self.assertFalse(r._scope_allows({"pipeline_id": 1, "status_id": 11}, config))
        note = r._note_text(99, [{"id": 1, "name": "Первый", "url": "https://amo/1"}, {"id": 2, "name": "Второй", "url": "https://amo/2"}])
        self.assertEqual(note, "Найдено дублей: 2\nhttps://amo/1\nhttps://amo/2")
        self.assertNotIn("Nexus", note)

    def test_retry_schedule_fits_two_minute_window(self):
        self.assertLess(sum(r.RETRY_DELAYS), r.MAX_EVENT_AGE_SECONDS)

    def test_latest_duplicate_is_selected_by_creation_time_then_id(self):
        duplicate = r._latest_duplicate(
            [
                {"id": 30, "created_at": 100},
                {"id": 20, "created_at": 200},
                {"id": 21, "created_at": 200},
            ]
        )
        self.assertEqual(duplicate["id"], 21)

    def test_unsorted_status_is_detected_from_catalog_type(self):
        catalog = {
            "pipelines": [
                {"id": "1", "statuses": [{"id": "10", "type": 1}, {"id": "11", "type": 0}]}
            ]
        }
        self.assertTrue(r._is_unsorted_lead({"pipeline_id": 1, "status_id": 10}, catalog))
        self.assertFalse(r._is_unsorted_lead({"pipeline_id": 1, "status_id": 11}, catalog))


class AsyncOutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_poll_registers_only_recent_lead_events(self):
        old_config, old_recent, old_register, old_resume = (
            r._config, r._recent_created_leads, r._register_event, r._resume_accepted_leads
        )
        registered = []

        async def config():
            return {"enabled": True}

        async def recent(_config):
            return [
                {"id": "101", "pipeline_id": "", "status_id": "", "event_id": "e1", "created_at": "100"},
                {"id": "102", "pipeline_id": "", "status_id": "", "event_id": "e2", "created_at": "101"},
            ], ""

        async def register(item, raw):
            registered.append((item, json.loads(raw)))
            return item["id"] == "101", "queued"

        async def resume(_config):
            return 0, 0, ""

        r._config, r._recent_created_leads, r._register_event, r._resume_accepted_leads = config, recent, register, resume
        try:
            created = await r._fallback_poll_once()
        finally:
            r._config, r._recent_created_leads, r._register_event, r._resume_accepted_leads = (
                old_config, old_recent, old_register, old_resume
            )
        self.assertEqual(created, 1)
        self.assertEqual([item[0]["id"] for item in registered], ["101", "102"])
        self.assertTrue(all(item[1]["source"] == "amo_events_fallback" for item in registered))

    async def test_event_registration_is_atomic_and_one_shot(self):
        old_db, old_schedule = r._db_path, r._schedule
        scheduled = []
        with tempfile.TemporaryDirectory() as directory:
            r._db_path = str(Path(directory) / "module.db")
            r._schedule = lambda lead_id, received_at=None: scheduled.append((lead_id, received_at)) or True
            try:
                await r._init_db()
                first = await r._register_event({"id": "55", "pipeline_id": "1", "status_id": "10"}, "{}")
                second = await r._register_event({"id": "55", "pipeline_id": "1", "status_id": "10"}, "{}")
                with sqlite3.connect(r._db_path) as db:
                    count = db.execute("SELECT COUNT(*) FROM events WHERE lead_id='55'").fetchone()[0]
            finally:
                r._db_path, r._schedule = old_db, old_schedule
        self.assertEqual(first, (True, "queued"))
        self.assertEqual(second, (False, "queued"))
        self.assertEqual(count, 1)
        self.assertEqual(len(scheduled), 1)

    async def test_find_duplicates_rechecks_candidates_exactly(self):
        config = {
            "source_scope": {"all": True, "statuses": []},
            "search": {"operator": "OR", "groups": [{"name": "phone", "operator": "AND", "conditions": [PHONE]}]},
            "base_tags": ["Дубль?"],
            "state_rules": [],
        }
        bundles = {
            1: ({"id": 1, "pipeline_id": 1, "status_id": 10}, [contact("89991234567")], ""),
            2: ({"id": 2, "name": "Дубль", "pipeline_id": 1, "status_id": 142, "created_at": 200, "responsible_user_id": 7}, [contact("+79991234567")], ""),
            3: ({"id": 3, "name": "Шум", "pipeline_id": 1, "status_id": 10}, [contact("+79990000000")], ""),
            4: ({"id": 4, "name": "Закрытый", "pipeline_id": 1, "status_id": 143, "created_at": 100}, [contact("+79991234567")], ""),
            5: ({"id": 5, "name": "Удалённый", "pipeline_id": 1, "status_id": 10, "is_deleted": True}, [contact("+79991234567")], ""),
        }
        old_bundle, old_ids, old_catalog, old_env = r._lead_bundle, r._candidate_ids, r._catalog, r._env

        async def bundle(lead_id, _config):
            return bundles[lead_id]

        async def ids(*_args):
            return [2, 3, 4, 5], ""

        async def catalog(_config):
            return {"pipelines": [{"id": "1", "name": "Продажи", "statuses": [{"id": "142", "name": "Успешно"}, {"id": "143", "name": "Закрыто и не реализовано"}]}], "users": [{"id": "7", "name": "Анна"}]}

        r._lead_bundle, r._candidate_ids, r._catalog = bundle, ids, catalog
        r._env = lambda: {"base_url": "https://example.amocrm.ru", "token": "x", "secret": ""}
        try:
            outcome, details, error = await r._find_duplicates(1, config)
        finally:
            r._lead_bundle, r._candidate_ids, r._catalog, r._env = old_bundle, old_ids, old_catalog, old_env
        self.assertEqual((outcome, error), ("ready", ""))
        self.assertEqual([item["id"] for item in details["duplicates"]], ["2", "4"])
        self.assertEqual(details["duplicates"][0]["created_at"], 200)
        self.assertEqual(details["duplicates"][0]["responsible_name"], "Анна")

    async def test_apply_result_adds_tags_without_replacement_and_one_note(self):
        calls = []
        old_request, old_exists = r._amo_request, r._note_exists

        async def request(method, path, config, payload=None):
            calls.append((method, path, payload))
            return {"ok": True}, "", 200

        async def note_exists(*_args):
            return False, ""

        r._amo_request, r._note_exists = request, note_exists
        try:
            result, error = await r._apply_result(
                10,
                {"pipeline_id": 1, "status_id": 10},
                [{"id": 20, "url": "https://amo/20"}],
                ["Дубль?", "В работе"],
                {},
            )
        finally:
            r._amo_request, r._note_exists = old_request, old_exists
        self.assertEqual(error, "")
        self.assertTrue(result["note_created"])
        self.assertEqual(calls[0][0:2], ("PATCH", "/api/v4/leads/10"))
        self.assertEqual(
            calls[0][2],
            {
                "tags_to_add": [{"name": "Дубль?"}, {"name": "В работе"}],
                "pipeline_id": 1,
                "status_id": 10,
            },
        )
        self.assertEqual(calls[1][0:2], ("POST", "/api/v4/leads/10/notes"))

    async def test_apply_result_copies_owner_from_latest_duplicate_once(self):
        calls = []
        old_request, old_exists = r._amo_request, r._note_exists

        async def request(method, path, config, payload=None):
            calls.append((method, path, payload))
            return {"ok": True}, "", 200

        async def note_exists(*_args):
            return True, ""

        r._amo_request, r._note_exists = request, note_exists
        duplicates = [
            {"id": 20, "created_at": 100, "responsible_user_id": 5, "url": "https://amo/20"},
            {"id": 30, "created_at": 200, "responsible_user_id": 7, "url": "https://amo/30"},
        ]
        try:
            result, error = await r._apply_result(
                10,
                {"pipeline_id": 1, "status_id": 66041242},
                duplicates,
                ["Дубль"],
                {"copy_responsible_from_latest_duplicate": True},
            )
        finally:
            r._amo_request, r._note_exists = old_request, old_exists
        self.assertEqual(error, "")
        self.assertEqual(result["responsible_user_id"], 7)
        self.assertEqual(result["responsible_source_lead_id"], 30)
        self.assertEqual(
            calls[0][2],
            {
                "tags_to_add": [{"name": "Дубль"}],
                "responsible_user_id": 7,
                "pipeline_id": 1,
                "status_id": 66041242,
            },
        )
        self.assertNotIn("new_status_id", calls[0][2])

    async def test_apply_result_preserves_owner_chosen_during_manual_accept(self):
        calls = []
        old_request, old_exists = r._amo_request, r._note_exists

        async def request(method, path, config, payload=None):
            calls.append((method, path, payload))
            return {"ok": True}, "", 200

        async def note_exists(*_args):
            return True, ""

        r._amo_request, r._note_exists = request, note_exists
        try:
            result, error = await r._apply_result(
                10,
                {"pipeline_id": 1, "status_id": 11, "responsible_user_id": 6269974},
                [{"id": 20, "created_at": 100, "responsible_user_id": 7, "url": "https://amo/20"}],
                ["Дубль"],
                {"copy_responsible_from_latest_duplicate": True},
            )
        finally:
            r._amo_request, r._note_exists = old_request, old_exists
        self.assertEqual(error, "")
        self.assertNotIn("responsible_user_id", result)
        self.assertNotIn("responsible_user_id", calls[0][2])
        self.assertEqual(calls[0][2]["status_id"], 11)

    async def test_waiting_unsorted_event_resumes_after_manual_accept(self):
        old_db, old_catalog, old_request, old_schedule = r._db_path, r._catalog, r._amo_request, r._schedule
        scheduled = []
        with tempfile.TemporaryDirectory() as directory:
            r._db_path = str(Path(directory) / "module.db")
            try:
                await r._init_db()
                with sqlite3.connect(r._db_path) as db:
                    db.execute(
                        "INSERT INTO events(lead_id,received_at,state) VALUES(?,?,?)",
                        ("55", r._now(), "waiting_unsorted"),
                    )
                    db.commit()

                async def catalog(_config):
                    return {"pipelines": [{"id": "1", "statuses": [{"id": "10", "type": 1}, {"id": "11", "type": 0}]}]}

                async def request(method, path, config, payload=None):
                    return {"id": 55, "pipeline_id": 1, "status_id": 11, "responsible_user_id": 99}, "", 200

                r._catalog, r._amo_request = catalog, request
                r._schedule = lambda lead_id, received_at=None: scheduled.append((lead_id, received_at)) or True
                resumed, waiting, error = await r._resume_accepted_leads({})
                with sqlite3.connect(r._db_path) as db:
                    row = db.execute(
                        "SELECT state,attempts,source_status_id,error FROM events WHERE lead_id='55'"
                    ).fetchone()
            finally:
                r._db_path, r._catalog, r._amo_request, r._schedule = old_db, old_catalog, old_request, old_schedule
        self.assertEqual((resumed, waiting, error), (1, 1, ""))
        self.assertEqual(row, ("queued", 0, "11", ""))
        self.assertEqual(scheduled[0][0], 55)

    async def test_unsorted_duplicate_waits_without_writing_to_amo(self):
        old_config, old_find, old_update, old_apply = r._config, r._find_duplicates, r._event_update, r._apply_result
        old_delays = r.RETRY_DELAYS
        updates = []

        async def config():
            return {"enabled": True, "base_tags": ["Дубль"], "state_rules": []}

        async def find(_lead_id, _config):
            return "ready", {
                "lead": {"id": 55, "pipeline_id": 1, "status_id": 10, "responsible_user_id": 0},
                "duplicates": [{"id": "20", "pipeline_id": "1", "status_id": "11"}],
                "source_is_unsorted": True,
            }, ""

        async def update(lead_id, **values):
            updates.append((lead_id, values))

        async def apply(*_args):
            self.fail("Unsorted lead must not be patched")

        r._config, r._find_duplicates, r._event_update, r._apply_result = config, find, update, apply
        r.RETRY_DELAYS = (0,)
        try:
            await r._process_lead(55, r._now())
        finally:
            r._config, r._find_duplicates, r._event_update, r._apply_result = old_config, old_find, old_update, old_apply
            r.RETRY_DELAYS = old_delays
        self.assertEqual(updates[-1][1]["state"], "waiting_unsorted")
        self.assertEqual(updates[-1][1]["duplicate_count"], 1)


if __name__ == "__main__":
    unittest.main()
