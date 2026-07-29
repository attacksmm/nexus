import asyncio
import json
import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from module_course_chat_creator import router as module


def test_default_date_is_current_moscow_date() -> None:
    before = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y")
    value = module._today_moscow()
    after = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y")
    assert value in {before, after}


def test_vk_response_helpers_accept_legacy_and_current_shapes() -> None:
    assert module.VK_API_VERSION == "5.199"
    assert module._vk_created_chat_id(17) == 17
    assert module._vk_created_chat_id({"chat_id": 18, "peer_ids": []}) == 18
    assert module._vk_message_reference(31) == (31, None)
    assert module._vk_message_reference(
        {"message_id": 32, "conversation_message_id": 7}
    ) == (32, 7)


def test_chat_link_row_matches_exact_stream_number() -> None:
    rows = [
        ["Поток 56", "https://example.test/56"],
        ["Поток 156", "https://example.test/156"],
    ]
    assert module._chat_link_row(rows, "56") == 1
    assert module._chat_link_row(rows, "156") == 2
    assert module._chat_link_row(rows, "57") is None


def test_sheet_sync_preserves_manual_links_and_never_publishes_generated_vk_link() -> None:
    assert module._sheet_link_write_value(
        "vk", "https://vk.me/join/manual-250", "https://vk.me/join/generated"
    ) == ("https://vk.me/join/manual-250", "preserved")
    assert module._sheet_link_write_value(
        "vk", "", "https://vk.me/join/generated"
    ) == ("", "waiting_manual_link")
    assert module._sheet_link_write_value(
        "telegram", "", "https://t.me/generated"
    ) == ("https://t.me/generated", "filled")


def test_chat_link_sync_waits_for_complete_vk_telegram_pair() -> None:
    async def scenario() -> None:
        with patch.object(
            module,
            "_ready_chat_pair",
            return_value={"vk": {"link": "https://vk.me/join/test"}},
        ):
            result = await module._sync_chat_pair_to_sheet("puppy", "57", test_mode=False)
        assert result == {"ok": True, "status": "waiting_pair", "missing": ["telegram"]}

    asyncio.run(scenario())


def test_chat_link_sync_status_requires_existing_credentials_file() -> None:
    with tempfile.TemporaryDirectory() as directory:
        credentials = Path(directory) / "service-account.json"
        previous = os.environ.get("COURSE_CHAT_CREATOR_GOOGLE_CREDENTIALS_FILE")
        os.environ["COURSE_CHAT_CREATOR_GOOGLE_CREDENTIALS_FILE"] = str(credentials)
        try:
            assert module._chat_links_sync_status()["configured"] is False
            credentials.write_text("{}", encoding="utf-8")
            assert module._chat_links_sync_status()["configured"] is True
        finally:
            if previous is None:
                os.environ.pop("COURSE_CHAT_CREATOR_GOOGLE_CREDENTIALS_FILE", None)
            else:
                os.environ["COURSE_CHAT_CREATOR_GOOGLE_CREDENTIALS_FILE"] = previous


def test_chat_link_sheet_failure_does_not_block_direct_nexus_delivery() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as directory:
            credentials = Path(directory) / "service-account.json"
            credentials.write_text("{}", encoding="utf-8")
            with (
                patch.object(
                    module,
                    "_ready_chat_pair",
                    return_value={
                        "vk": {"link": "https://vk.me/join/57"},
                        "telegram": {"link": "https://t.me/57"},
                    },
                ),
                patch.object(module, "_chat_links_credentials_path", return_value=credentials),
                patch.object(module, "_sync_chat_pair_to_sheet_sync", side_effect=RuntimeError("403 Forbidden")),
            ):
                result = await module._sync_chat_pair_to_sheet("puppy", "57", test_mode=False)

        assert result["ok"] is True
        assert result["sheet_sync_ok"] is False
        assert result["status"] == "direct_ready_sheet_error"

    asyncio.run(scenario())


def test_vk_student_cohort_uses_exact_flow_roster() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        flow_db = root / "flows.db"
        customer_db = root / "customers.db"
        with sqlite3.connect(flow_db) as db:
            db.execute("CREATE TABLE flow_students_cache(key TEXT,value_json TEXT,updated_at TEXT)")
            db.execute(
                "INSERT INTO flow_students_cache VALUES(?,?,?)",
                (
                    "cache",
                    json.dumps(
                        {
                            "items": [
                                {
                                    "course_key": "puppy",
                                    "stream": "56",
                                    "sheet_title": "Щ56 (22.07)",
                                    "sheet_id": 1161471765,
                                    "sheet_url": "https://docs.google.com/sheet#gid=1161471765",
                                    "students": [
                                        {"email": "one@example.com", "source_record_id": 1},
                                        {"email": "two@example.com", "source_record_id": 2},
                                        {"email": "standard@example.com", "source_record_id": 4, "tariff": "Стандарт"},
                                    ],
                                },
                                {
                                    "course_key": "puppy",
                                    "stream": "55",
                                    "students": [{"email": "old@example.com", "source_record_id": 3}],
                                },
                            ]
                        }
                    ),
                    "2026-07-27T19:45:59Z",
                ),
            )
        with sqlite3.connect(customer_db) as db:
            db.execute(
                "CREATE TABLE cdb_getcourse_orders(id INTEGER PRIMARY KEY,custom_fields TEXT,updated_at TEXT,created_at TEXT)"
            )
            for row_id, email, vk_id, tariff in (
                (1, "one@example.com", "101", "Премиум"),
                (2, "two@example.com", "", "Премиум"),
                (3, "old@example.com", "999", "Премиум"),
                (4, "standard@example.com", "404", "Стандарт"),
            ):
                db.execute(
                    "INSERT INTO cdb_getcourse_orders VALUES(?,?,?,?)",
                    (
                        row_id,
                        json.dumps(
                            {
                                "email": email,
                                "status": "Завершен",
                                "payment_state": "paid",
                                "vk_id": vk_id,
                                "gc_user_id": str(row_id),
                                "title": f"Курс. Тариф {tariff}",
                            }
                        ),
                        "2026-07-27T19:00:00Z",
                        "2026-07-27T18:00:00Z",
                    ),
                )

        def sibling(_env_key: str, module_id: str, _filename: str):
            return flow_db if module_id == "getcourse-chat-fields" else customer_db

        with patch.object(module, "_sibling_module_db", side_effect=sibling):
            result = module._vk_student_cohort("puppy", "56")

        assert result["available"] is True
        assert result["sheet_title"] == "Щ56 (22.07)"
        assert result["total"] == 2
        assert result["sheet_students"] == 3
        assert result["standard_excluded"] == 1
        assert result["with_vk"] == 1
        assert result["without_vk"] == 1
        assert result["vk_ids"] == [101]
        assert 999 not in result["vk_ids"]
        assert 404 not in result["vk_ids"]


def test_vk_entitlement_cohort_uses_only_v2_eligible_processed_orders() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        flow_db = root / "flows.db"
        customer_db = root / "customers.db"
        with sqlite3.connect(flow_db) as db:
            db.execute(
                """CREATE TABLE processed_orders(
                    source_record_id INTEGER,course_key TEXT,stream TEXT,customer_ok INTEGER,
                    vk_link TEXT,tg_link TEXT,details_json TEXT
                )"""
            )
            db.executemany(
                "INSERT INTO processed_orders VALUES(?,?,?,?,?,?,?)",
                [
                    (1, "puppy", "56", 1, "https://vk/56", "https://t.me/56", json.dumps({"entitlement": {"version": 2, "eligible": True}})),
                    (2, "puppy", "56", 1, "https://vk/56", "https://t.me/56", json.dumps({"entitlement": {"version": 1, "eligible": True}})),
                    (3, "puppy", "56", 1, "https://vk/56", "https://t.me/56", json.dumps({"entitlement": {"version": 2, "eligible": False}})),
                    (4, "puppy", "55", 1, "https://vk/55", "https://t.me/55", json.dumps({"entitlement": {"version": 2, "eligible": True}})),
                    (5, "puppy", "56", 1, "https://vk/56", "https://t.me/56", json.dumps({"entitlement": {"version": 2, "eligible": True}})),
                ],
            )
        with sqlite3.connect(customer_db) as db:
            db.execute("CREATE TABLE cdb_getcourse_orders(id INTEGER PRIMARY KEY,custom_fields TEXT)")
            db.executemany(
                "INSERT INTO cdb_getcourse_orders VALUES(?,?)",
                [
                    (1, json.dumps({"gc_user_id": "11", "vk_id": "101"})),
                    (2, json.dumps({"gc_user_id": "12", "vk_id": "202"})),
                    (3, json.dumps({"gc_user_id": "13", "vk_id": "303"})),
                    (4, json.dumps({"gc_user_id": "14", "vk_id": "404"})),
                    (5, json.dumps({"gc_user_id": "15", "vk_id": ""})),
                ],
            )

        def sibling(_env_key: str, module_id: str, _filename: str):
            return flow_db if module_id == "getcourse-chat-fields" else customer_db

        with patch.object(module, "_sibling_module_db", side_effect=sibling):
            result = module._vk_processed_entitlement_cohort("puppy", "56")

        assert result["available"] is True
        assert result["total"] == 2
        assert result["with_vk"] == 1
        assert result["without_vk"] == 1
        assert result["vk_ids"] == [101]


def test_vk_invite_dispatch_uses_latest_run_and_deduplicates_old_outcomes() -> None:
    rows = [
        {
            "id": 34,
            "course_key": "dog",
            "stream_number": "54",
            "response_json": json.dumps(
                {"student_invites": {"sent_vk_ids": [101], "not_allowed_vk_ids": [102]}}
            ),
        },
        {
            "id": 36,
            "course_key": "dog",
            "stream_number": "54",
            "response_json": json.dumps(
                {"student_invites": {"sent_vk_ids": [103], "joined_vk_ids": [104]}}
            ),
        },
        {
            "id": 38,
            "course_key": "puppy",
            "stream_number": "56",
            "response_json": "{}",
        },
    ]
    result = module._canonical_vk_invite_runs(rows)
    assert [item["id"] for item in result] == [36, 38]
    assert result[0]["historical_completed_vk_ids"] == [101, 102, 103, 104]
    assert result[1]["historical_completed_vk_ids"] == []


def test_vk_invite_dispatch_excludes_legacy_user_owned_runs() -> None:
    rows = [
        {
            "id": 36,
            "course_key": "dog",
            "stream_number": "54",
            "response_json": json.dumps({"student_invites": {"pending_vk_ids": [101]}}),
        },
        {
            "id": 42,
            "course_key": "puppy",
            "stream_number": "1000",
            "response_json": json.dumps(
                {"owner_group_id": 225075265, "student_invites": {"pending_vk_ids": [202]}}
            ),
        },
        {
            "id": 43,
            "course_key": "puppy",
            "stream_number": "1001",
            "response_json": json.dumps(
                {"owner_group_id": 999, "student_invites": {"pending_vk_ids": [303]}}
            ),
        },
    ]
    result = module._community_owned_vk_invite_runs(rows, 225075265)
    assert [item["id"] for item in result] == [42]


def test_vk_test_mode_selects_only_tech_support_from_configured_staff() -> None:
    selected = {
        "admins": [
            {"id": 19, "name": "Никита", "vk_id": "741919467"},
            {"id": 20, "name": "Андрей", "vk_id": "11335495"},
        ],
        "authors": [{"id": 21, "name": "Анна", "vk_id": "765938"}],
        "kurators": [{"id": 22, "name": "Ирина", "vk_id": "413314992"}],
        "techs": [{"id": 23, "name": "Техподдержка", "vk_id": "1105209997"}],
    }

    assert [person["vk_id"] for person in module._vk_staff_for_mode(selected, test_mode=True)] == ["1105209997"]
    assert len(module._vk_staff_for_mode(selected, test_mode=False)) == 5


def test_vk_test_chat_invites_only_tech_support_and_never_reads_students() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        selected = {
            "admins": [
                {"id": 19, "name": "Никита", "vk_id": "741919467"},
                {"id": 20, "name": "Андрей", "vk_id": "11335495"},
            ],
            "authors": [{"id": 21, "name": "Анна", "vk_id": "765938"}],
            "kurators": [{"id": 22, "name": "Ирина", "vk_id": "413314992"}],
            "techs": [{"id": 23, "name": "Техподдержка", "vk_id": "1105209997"}],
        }

        async def vk_method(method: str, params: dict, token: str):
            calls.append((method, dict(params)))
            assert token == "group-token"
            if method == "messages.createChat":
                return {"chat_id": 45, "peer_ids": [1105209997]}
            if method == "messages.getInviteLink":
                return {"link": "https://vk.me/join/safe-test"}
            raise AssertionError(f"unexpected VK method: {method}")

        async def resolve_people(people, _token):
            assert [person["vk_id"] for person in people] == ["1105209997"]
            return [1105209997]

        async def admin_state(_peer_id, target_ids, _token):
            assert target_ids == [1105209997]
            return {"members": [{"id": 1105209997, "role": "admin"}], "missing_admins": []}

        async def promote(_peer_id, target_ids, _token):
            assert target_ids == [1105209997]
            return {"ok": True, "state": {"missing_admins": []}}

        async def initialize(row):
            result = dict(row["response"])
            result.update({"bootstrap_status": "ready", "followup_status": "ok", "welcome_pinned": True})
            return result

        async def fallback(user_ids, **_kwargs):
            assert user_ids == []
            return {"candidates": 0, "sent": 0, "not_allowed": 0, "failed": 0, "errors": []}

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        previous_group_id = os.environ.get("VK_GROUP_ID")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        os.environ["VK_GROUP_ID"] = "225075265"
        try:
            with (
                patch.object(module, "_course_by_input", return_value={"key": "puppy"}),
                patch.object(module, "_format_title", return_value="999. 27.07.2026 - Проверка"),
                patch.object(module, "_selected_people", return_value=selected),
                patch.object(module, "_selected_curator_id", return_value=22),
                patch.object(module, "_resolve_vk_people_ids", side_effect=resolve_people),
                patch.object(module, "_vk_student_cohort", side_effect=AssertionError("test mode must not read students")),
                patch.object(module, "_vk_admin_state", side_effect=admin_state),
                patch.object(module, "_vk_try_api_admins", side_effect=promote),
                patch.object(module, "_initialize_vk_chat_from_run", side_effect=initialize),
                patch.object(module, "_send_vk_invite_fallbacks", side_effect=fallback),
                patch.object(module, "_avatar_path", return_value=None),
                patch.object(module, "_record_run", return_value=41),
                patch.object(module, "_update_run"),
                patch.object(module, "_vk_method", side_effect=vk_method),
            ):
                result = await module._create_vk_chat(
                    {
                        "stream_number": "999",
                        "date_start": "27.07.2026",
                        "course_type": "puppy",
                        "test_mode": True,
                    },
                    trusted=True,
                )
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token
            if previous_group_id is None:
                os.environ.pop("VK_GROUP_ID", None)
            else:
                os.environ["VK_GROUP_ID"] = previous_group_id

        assert calls[0] == (
            "messages.createChat",
            {
                "title": "999. 27.07.2026 - Проверка",
                "group_id": 225075265,
                "user_ids": "1105209997",
            },
        )
        assert result["members_result"]["expected_staff_ids"] == [1105209997]
        assert result["members_result"]["student_requested"] == 0
        assert result["members_result"]["student_total"] == 0
        assert result["student_invites"]["getcourse_link_required"] == 0
        assert result["student_invites"]["community_messages"] == "not_needed"

    asyncio.run(scenario())


def test_vk_role_manager_dry_run_does_not_mutate() -> None:
    async def scenario() -> None:
        inventory = {
            "accessible": 2,
            "inaccessible": 1,
            "items": [
                {
                    "peer_id": 2000000017,
                    "chat_id": 17,
                    "title": "999. 27.07.2026 - Курс Щенок. Современный Собаковод",
                    "accessible": True,
                    "target_present": True,
                    "target_role": "member",
                },
                {
                    "peer_id": 2000000021,
                    "chat_id": 21,
                    "title": "998. 27.07.2026 - Курс Щенок. Современный Собаковод",
                    "accessible": True,
                    "target_present": False,
                    "target_role": "",
                },
                {
                    "peer_id": 2000000132,
                    "chat_id": 132,
                    "title": "56. 22.07.2026 - Курс Щенок. Современный Собаковод",
                    "accessible": False,
                    "status": "inaccessible",
                },
            ],
        }
        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_resolve_vk_target_id", return_value=1105209997),
                patch.object(module, "_vk_course_chat_inventory", return_value=inventory),
                patch.object(module, "_vk_method") as vk_method,
            ):
                result = await module._manage_vk_course_chats(
                    "https://vk.ru/tehpod_sobakovodpro",
                    action="grant_admin",
                    dry_run=True,
                )
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert [item["status"] for item in result["items"]] == [
            "would_grant_admin",
            "not_member",
            "inaccessible",
        ]
        vk_method.assert_not_called()

    asyncio.run(scenario())


def test_vk_role_manager_revokes_and_verifies_role() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        inventory = {
            "accessible": 1,
            "inaccessible": 0,
            "items": [
                {
                    "peer_id": 2000000021,
                    "chat_id": 21,
                    "title": "998. 27.07.2026 - Курс Щенок. Современный Собаковод",
                    "accessible": True,
                    "target_present": True,
                    "target_role": "admin",
                }
            ],
        }

        async def vk_method(method: str, params: dict, _token: str):
            calls.append((method, dict(params)))
            if method == "messages.setMemberRole":
                return 1
            if method == "messages.getConversationMembers":
                return {"items": [{"member_id": 741919467, "role": "member"}]}
            raise AssertionError(method)

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_resolve_vk_target_id", return_value=741919467),
                patch.object(module, "_vk_course_chat_inventory", return_value=inventory),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(module.asyncio, "sleep", return_value=None),
            ):
                result = await module._manage_vk_course_chats(
                    "https://vk.ru/attackpng",
                    action="revoke_admin",
                    dry_run=False,
                )
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert result["items"][0]["status"] == "admin_revoked"
        assert calls[0] == (
            "messages.setMemberRole",
            {"peer_id": 2000000021, "member_id": 741919467, "role": "member"},
        )

    asyncio.run(scenario())


def test_vk_chat_creation_seeds_paid_students_and_falls_back_for_rejected_ids() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        get_by_calls = 0

        async def vk_method(method: str, params: dict, token: str):
            nonlocal get_by_calls
            calls.append((method, dict(params)))
            assert token == "group-token"
            if method == "messages.createChat":
                return {"chat_id": 44, "peer_ids": [101, 201]}
            if method == "messages.getConversationMembers":
                return {
                    "items": [
                        {"member_id": -225075265, "role": "owner"},
                        {"member_id": 101, "role": "member"},
                        {"member_id": 201, "role": "member"},
                    ]
                }
            if method == "messages.getInviteLink":
                return {"link": "https://vk.me/join/test"}
            if method == "messages.getByConversationMessageId":
                get_by_calls += 1
                if get_by_calls < 3:
                    return {"count": 0, "items": []}
                return {
                    "count": 1,
                    "items": [
                        {
                            "id": 0,
                            "conversation_message_id": 4,
                            "from_id": -225075265,
                            "text": "Добро пожаловать",
                            "attachments": [],
                        }
                    ],
                }
            if method == "messages.send":
                return 0
            if method == "messages.pin":
                return 1
            raise AssertionError(f"unexpected VK method: {method}")

        async def resolve_people(_people, _token):
            return [101, 102]

        async def no_sleep(_seconds):
            return None

        async def promote(_peer_id, target_ids, _token):
            assert target_ids == [101]
            return {"ok": True, "state": {"missing_admins": []}}

        async def link_sync(_course_key, _stream_number, *, test_mode):
            assert test_mode is False
            return {"ok": True, "status": "waiting_pair", "missing": ["telegram"]}

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        previous_group_id = os.environ.get("VK_GROUP_ID")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        os.environ["VK_GROUP_ID"] = "225075265"
        try:
            with (
                patch.object(module, "_course_by_input", return_value={"key": "puppy"}),
                patch.object(module, "_format_title", return_value="15. 17.03.2026 - Щенок"),
                patch.object(
                    module,
                    "_selected_people",
                    return_value={"admins": [{}], "authors": [], "kurators": [], "techs": []},
                ),
                patch.object(module, "_selected_curator_id", return_value=0),
                patch.object(module, "_resolve_vk_people_ids", side_effect=resolve_people),
                patch.object(
                    module,
                    "_vk_processed_entitlement_cohort",
                    return_value={
                        "available": True,
                        "source": "getcourse-chat-fields",
                        "total": 3,
                        "with_vk": 2,
                        "without_vk": 1,
                        "vk_ids": [201, 202],
                    },
                ),
                patch.object(module, "_vk_try_api_admins", side_effect=promote),
                patch.object(module, "_sync_chat_pair_to_sheet", side_effect=link_sync),
                patch.object(module, "_avatar_path", return_value=None),
                patch.object(module, "_asset_path", return_value=None),
                patch.object(module, "_render_template", return_value="Добро пожаловать"),
                patch.object(module, "_record_run", return_value=40),
                patch.object(module, "_update_run"),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(module.asyncio, "sleep", side_effect=no_sleep),
            ):
                result = await module._create_vk_chat(
                    {"stream_number": "15", "date_start": "17 марта", "course_type": "puppy"},
                    trusted=True,
                )
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token
            if previous_group_id is None:
                os.environ.pop("VK_GROUP_ID", None)
            else:
                os.environ["VK_GROUP_ID"] = previous_group_id

        assert result["owner_group_id"] == 225075265
        assert result["join_mode"] == "initial_members_and_invite_link"
        assert result["group_link"] == "https://vk.me/join/test"
        assert result["bootstrap_status"] == "ready"
        assert result["followup_status"] == "needs_members"
        assert result["welcome_cmid"] == 4
        assert result["welcome_pinned"] is True
        assert result["members_result"]["expected_staff_ids"] == [101, 102]
        assert result["members_result"]["student_added"] == 1
        assert result["members_result"]["student_not_added"] == 1
        assert result["student_invites"]["delivery"] == "community_message_and_client_chat_links_page"
        assert result["student_invites"]["community_messages"] == "waiting_manual_link"
        assert result["student_invites"]["pending_vk_ids"] == [202]
        assert result["staff_invites"]["delivery"] == "history_link"
        assert result["staff_invites"]["community_messages"] == "disabled"
        assert result["student_invites"]["getcourse_link_required"] == 2
        assert result["link_sync"]["status"] == "waiting_pair"
        assert not any(method == "messages.isMessagesFromGroupAllowed" for method, _params in calls)
        assert calls[0] == (
            "messages.createChat",
            {
                "title": "15. 17.03.2026 - Щенок",
                "group_id": 225075265,
                "user_ids": "101,102,201,202",
            },
        )
        assert [method for method, _params in calls] == [
            "messages.createChat",
            "messages.getConversationMembers",
            "messages.getInviteLink",
            "messages.getByConversationMessageId",
            "messages.send",
            "messages.getByConversationMessageId",
            "messages.getByConversationMessageId",
            "messages.pin",
        ]
        assert all(method != "messages.addChatUser" for method, _params in calls)

    asyncio.run(scenario())


def test_vk_invite_fallback_sends_only_when_community_messages_are_allowed() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []

        async def vk_method(method: str, params: dict, _token: str):
            calls.append((method, dict(params)))
            if method == "messages.isMessagesFromGroupAllowed":
                return {"is_allowed": 1 if params["user_id"] == 201 else 0}
            if method == "messages.send":
                return 1
            raise AssertionError(method)

        with (
            patch.object(module, "_render_template", return_value="Ссылка https://vk.me/join/test"),
            patch.object(module, "_vk_method", side_effect=vk_method),
            patch.object(module.asyncio, "sleep", return_value=None),
        ):
            result = await module._send_vk_invite_fallbacks(
                [201, 202],
                invite_link="https://vk.me/join/test",
                group_id=225075265,
                token="group-token",
                course={"title": "Курс", "key": "puppy", "choice": "1"},
                stream_number="15",
                date_start="17.03.2026",
                selected={"admins": [], "authors": [], "kurators": [], "techs": []},
            )

        assert result == {
            "candidates": 2,
            "sent": 1,
            "not_allowed": 1,
            "failed": 0,
            "errors": [],
            "sent_user_ids": [201],
            "not_allowed_user_ids": [202],
            "failed_user_ids": [],
        }
        assert [method for method, _params in calls].count("messages.send") == 1

    asyncio.run(scenario())


def test_manual_vk_invite_link_uses_exact_sheet_stream_and_rejects_generated_link() -> None:
    rows = [
        ["Щенок 56", "https://vk.me/join/manual-250"],
        ["Щенок 57", "https://vk.me/join/generated"],
        ["Щенок 58", "https://vk.me/join/other"],
    ]

    assert module._manual_vk_link_from_rows(rows, "56", "https://vk.me/join/generated") == (
        "https://vk.me/join/manual-250"
    )
    assert module._manual_vk_link_from_rows(rows, "57", "https://vk.me/join/generated") == ""


def test_vk_staff_invites_send_join_link_only_when_messages_are_allowed() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []

        async def vk_method(method: str, params: dict, _token: str):
            calls.append((method, dict(params)))
            if method == "messages.isMessagesFromGroupAllowed":
                return {"is_allowed": 1 if params["user_id"] == 101 else 0}
            if method == "messages.send":
                assert params["peer_id"] == 101
                assert "https://vk.me/join/test" in params["message"]
                assert "права администратора будут выданы автоматически" in params["message"]
                return 1
            raise AssertionError(method)

        with (
            patch.object(module, "_vk_method", side_effect=vk_method),
            patch.object(module.asyncio, "sleep", return_value=None),
        ):
            result = await module._send_vk_staff_invites(
                [101, 102],
                invite_link="https://vk.me/join/test",
                group_id=225075265,
                token="group-token",
            )

        assert result == {"candidates": 2, "sent": 1, "not_allowed": 1, "failed": 0, "errors": []}
        assert [method for method, _params in calls].count("messages.send") == 1

    asyncio.run(scenario())


def test_first_message_bootstrap_uses_test_welcome_and_pins_once() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        updates: list[tuple[str, dict, str]] = []

        async def vk_method(method: str, params: dict, token: str):
            calls.append((method, dict(params)))
            assert token == "group-token"
            if method == "messages.send" and "attachment" in params:
                return {"message_id": 80, "conversation_message_id": 8}
            if method == "messages.send":
                return {"message_id": 81, "conversation_message_id": 9}
            if method == "messages.getByConversationMessageId":
                return {"count": 0, "items": []}
            if method == "messages.pin":
                return 1
            raise AssertionError(f"unexpected VK method: {method}")

        async def upload_photo(_peer_id, _path, _token):
            return "photo-1_2"

        def update_run(_run_id, status, response, *, error=""):
            updates.append((status, dict(response), error))

        row = {
            "id": 40,
            "stream_number": "999",
            "date_start": "27.07.2026",
            "course_key": "puppy",
            "test_mode": 1,
            "request": {"stream_number": "999", "date_start": "27.07.2026", "course_type": "puppy"},
            "response": {
                "peer_id": 2000000017,
                "owner_group_id": 225075265,
                "bootstrap_status": "waiting_for_message",
            },
        }
        previous_token = os.environ.get("VK_GROUP_TOKEN")
        previous_group_id = os.environ.get("VK_GROUP_ID")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        os.environ["VK_GROUP_ID"] = "225075265"
        try:
            with (
                patch.object(module, "_course_by_input", return_value={"key": "puppy", "title": "Щенок"}),
                patch.object(
                    module,
                    "_selected_people",
                    return_value={"admins": [], "authors": [], "kurators": [], "techs": []},
                ),
                patch.object(module, "_asset_path", return_value=module.Path("welcome.jpg")),
                patch.object(module, "_upload_vk_message_photo", side_effect=upload_photo),
                patch.object(module, "_render_template", return_value="Добро пожаловать") as render_template,
                patch.object(module, "_update_run", side_effect=update_run),
                patch.object(module, "_vk_method", side_effect=vk_method),
            ):
                result = await module._initialize_vk_chat_from_run(row)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token
            if previous_group_id is None:
                os.environ.pop("VK_GROUP_ID", None)
            else:
                os.environ["VK_GROUP_ID"] = previous_group_id

        assert result["bootstrap_status"] == "ready"
        assert result["welcome_photo_sent"] is True
        assert result["welcome_message_id"] == 80
        assert result["welcome_cmid"] == 8
        assert result["welcome_photo_cmid"] == 8
        assert result["welcome_message_has_photo"] is True
        assert result["welcome_pinned"] is True
        assert render_template.call_args.args[0] == "vk_test_welcome"
        actual_methods = [method for method, _params in calls]
        assert actual_methods == [
            "messages.getByConversationMessageId",
            "messages.send",
            "messages.pin",
        ], actual_methods
        assert calls[1][1]["message"] == "Добро пожаловать"
        assert calls[1][1]["attachment"] == "photo-1_2"
        assert calls[-1][1] == {"peer_id": 2000000017, "cmid": 8}
        assert updates[-1][0] == "ok"
        assert updates[-1][1]["bootstrap_status"] == "ready"

    asyncio.run(scenario())


def test_join_service_event_waits_for_first_message() -> None:
    async def scenario() -> None:
        persisted: list[tuple[str, dict]] = []
        event = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "peer_id": 2000000017,
                    "from_id": 1105209997,
                    "text": "",
                    "action": {"type": "chat_invite_user"},
                }
            },
        )
        row = {
            "id": 40,
            "response": {
                "peer_id": 2000000017,
                "owner_group_id": 225075265,
                "bootstrap_status": "waiting_for_message",
            },
        }

        def persist(_row, response, status, *, error=""):
            assert not error
            persisted.append((status, dict(response)))

        with (
            patch.object(module, "_vk_owned_run", return_value=row),
            patch.object(module, "_pending_vk_bootstrap_run", return_value=row),
            patch.object(module, "_persist_vk_bootstrap", side_effect=persist),
            patch.object(module, "_initialize_vk_chat_from_run") as initialize,
        ):
            await module._handle_vk_bootstrap_event(event)

        assert len(persisted) == 1
        assert persisted[0][0] == "waiting_for_message"
        assert persisted[0][1]["peer_id"] == 2000000017
        assert persisted[0][1]["bootstrap_status"] == "waiting_for_message"
        initialize.assert_not_called()

    asyncio.run(scenario())


def test_joined_staff_uses_current_configuration_for_automatic_promotion() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        updates: list[tuple[str, dict, str]] = []
        event = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "peer_id": 2000000017,
                    "from_id": 1105209997,
                    "text": "",
                    "action": {"type": "chat_invite_user_by_link", "member_id": 1105209997},
                }
            },
        )
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "response": {
                "peer_id": 2000000017,
                "owner_group_id": 225075265,
                "bootstrap_status": "ready",
                "members_result": {"expected_staff_ids": [741919467]},
            },
        }

        async def vk_method(method: str, params: dict, token: str):
            calls.append((method, dict(params)))
            assert token == "group-token"
            assert method == "messages.setMemberRole"
            return 1

        def update_run(_run_id, status, response, *, error=""):
            updates.append((status, dict(response), error))

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_vk_owned_run", return_value=row),
                patch.object(module, "_pending_vk_bootstrap_run", return_value=None),
                patch.object(
                    module,
                    "_selected_people",
                    return_value={
                        "admins": [{"vk_id": "1105209997"}],
                        "authors": [],
                        "kurators": [],
                        "techs": [],
                    },
                ),
                patch.object(module, "_resolve_vk_people_ids", return_value=[1105209997]),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(
                    module,
                    "_vk_admin_state",
                    return_value={"ok": True, "admins": [1105209997], "members": [], "missing_admins": []},
                ),
                patch.object(module, "_update_run", side_effect=update_run),
            ):
                await module._handle_vk_bootstrap_event(event)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert calls == [
            (
                "messages.setMemberRole",
                {"peer_id": 2000000017, "member_id": 1105209997, "role": "admin"},
            )
        ]
        assert updates[-1][0] == "ok"
        assert updates[-1][1]["staff_roles"]["1105209997"]["status"] == "admin"
        assert updates[-1][1]["admin_result"]["pending_join_ids"] == []
        assert updates[-1][2] == ""

    asyncio.run(scenario())


def test_replaced_pin_action_restores_course_pin() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        updates: list[dict] = []
        event = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "peer_id": 2000000017,
                    "from_id": 123,
                    "text": "",
                    "action": {"type": "chat_pin_message", "conversation_message_id": 13},
                }
            },
        )
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "response": {
                "peer_id": 2000000017,
                "owner_group_id": 225075265,
                "bootstrap_status": "ready",
                "welcome_cmid": 4,
                "welcome_pinned": True,
            },
        }

        async def vk_method(method: str, params: dict, token: str):
            calls.append((method, dict(params)))
            assert method == "messages.pin"
            assert token == "group-token"
            return 1

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_vk_owned_run", return_value=row),
                patch.object(module, "_pending_vk_bootstrap_run", return_value=None),
                patch.object(
                    module,
                    "_vk_admin_state",
                    return_value={"ok": True, "admins": [], "members": [], "missing_admins": [123]},
                ),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(module, "_update_run", side_effect=lambda _id, _status, response, **_kwargs: updates.append(dict(response))),
            ):
                await module._handle_vk_bootstrap_event(event)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert calls == [("messages.pin", {"peer_id": 2000000017, "cmid": 4})]
        assert updates[-1]["pin_watchdog"]["restored_count"] == 1
        assert updates[-1]["pin_watchdog"]["trigger"] == "chat_pin_message"

    asyncio.run(scenario())


def test_course_pin_action_does_not_repin_itself() -> None:
    async def scenario() -> None:
        event = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "peer_id": 2000000017,
                    "from_id": 123,
                    "text": "",
                    "action": {"type": "chat_pin_message", "conversation_message_id": 4},
                }
            },
        )
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "response": {
                "peer_id": 2000000017,
                "owner_group_id": 225075265,
                "bootstrap_status": "ready",
                "welcome_cmid": 4,
                "welcome_pinned": True,
            },
        }

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_vk_owned_run", return_value=row),
                patch.object(module, "_pending_vk_bootstrap_run", return_value=None),
                patch.object(module, "_vk_method") as vk_method,
                patch.object(module, "_update_run") as update_run,
            ):
                await module._handle_vk_bootstrap_event(event)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        vk_method.assert_not_awaited()
        update_run.assert_not_called()

    asyncio.run(scenario())


def test_periodic_pin_reconcile_restores_only_changed_course_pin() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        updates: list[dict] = []
        rows = [
            {
                "id": 40,
                "status": "ok",
                "error": "",
                "response": {"peer_id": 2000000017, "welcome_cmid": 4},
            },
            {
                "id": 41,
                "status": "ok",
                "error": "",
                "response": {"peer_id": 2000000021, "welcome_cmid": 9},
            },
        ]
        checks = 0

        async def vk_method(method: str, params: dict, token: str):
            nonlocal checks
            calls.append((method, dict(params)))
            assert token == "group-token"
            if method == "messages.pin":
                assert params == {"peer_id": 2000000017, "cmid": 4}
                return 1
            assert method == "messages.getConversationsById"
            checks += 1
            if checks == 1:
                return {
                    "items": [
                        {
                            "conversation": {
                                "peer": {"id": 2000000017},
                                "chat_settings": {
                                    "pinned_message": {"conversation_message_id": 13}
                                },
                            }
                        },
                        {
                            "peer": {"id": 2000000021},
                            "chat_settings": {
                                "pinned_message": {"conversation_message_id": 9}
                            },
                        },
                    ]
                }
            return {
                "items": [
                    {
                        "conversation": {
                            "peer": {"id": 2000000017},
                            "chat_settings": {
                                "pinned_message": {"conversation_message_id": 4}
                            },
                        }
                    }
                ]
            }

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_vk_course_pin_rows", return_value=rows),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(
                    module,
                    "_update_run",
                    side_effect=lambda _id, _status, response, **_kwargs: updates.append(dict(response)),
                ),
            ):
                result = await module._reconcile_vk_course_pins_once()
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert result == {"ok": True, "checked": 2, "restored": 1, "suspended": 0, "failed": 0}
        assert [method for method, _params in calls] == [
            "messages.getConversationsById",
            "messages.pin",
            "messages.getConversationsById",
        ]
        assert updates[-1]["pin_watchdog"]["trigger"] == "periodic_reconcile"

    asyncio.run(scenario())


def test_admin_pin_change_suspends_automatic_restore() -> None:
    async def scenario() -> None:
        updates: list[dict] = []
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "response": {"peer_id": 2000000017, "welcome_cmid": 4, "welcome_pinned": True},
        }
        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(
                    module,
                    "_vk_admin_state",
                    return_value={"ok": True, "admins": [123], "members": [], "missing_admins": []},
                ),
                patch.object(module, "_vk_method") as vk_method,
                patch.object(
                    module,
                    "_update_run",
                    side_effect=lambda _id, _status, response, **_kwargs: updates.append(dict(response)),
                ),
            ):
                await module._restore_vk_course_pin(
                    row,
                    {
                        "peer_id": 2000000017,
                        "from_id": 123,
                        "action_type": "chat_pin_message",
                        "action_cmid": 13,
                    },
                )
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        vk_method.assert_not_awaited()
        assert updates[-1]["pin_watchdog"]["suspended_by_admin"] is True
        assert updates[-1]["pin_watchdog"]["suspended_by_admin_id"] == 123
        assert updates[-1]["welcome_pinned"] is False

    asyncio.run(scenario())


def test_periodic_pin_reconcile_skips_admin_override() -> None:
    async def scenario() -> None:
        rows = [
            {
                "id": 40,
                "status": "ok",
                "error": "",
                "response": {
                    "peer_id": 2000000017,
                    "welcome_cmid": 4,
                    "pin_watchdog": {"suspended_by_admin": True},
                },
            }
        ]

        async def vk_method(method: str, params: dict, _token: str):
            assert method == "messages.getConversationsById"
            return {
                "items": [
                    {
                        "conversation": {
                            "peer": {"id": 2000000017},
                            "chat_settings": {
                                "pinned_message": {"conversation_message_id": 13}
                            },
                        }
                    }
                ]
            }

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        try:
            with (
                patch.object(module, "_vk_course_pin_rows", return_value=rows),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(module, "_update_run") as update_run,
            ):
                result = await module._reconcile_vk_course_pins_once()
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token

        assert result == {"ok": True, "checked": 1, "restored": 0, "suspended": 1, "failed": 0}
        update_run.assert_not_called()

    asyncio.run(scenario())


def test_manual_pin_restore_clears_admin_override() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        persisted: list[dict] = []
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "response": {
                "peer_id": 2000000017,
                "welcome_cmid": 4,
                "pin_watchdog": {"suspended_by_admin": True, "restored_count": 2},
            },
        }

        async def vk_method(method: str, params: dict, _token: str):
            calls.append((method, dict(params)))
            if method == "messages.pin":
                return 1
            current_cmid = 13 if len(calls) == 1 else 4
            return {
                "items": [
                    {
                        "conversation": {
                            "peer": {"id": 2000000017},
                            "chat_settings": {
                                "pinned_message": {"conversation_message_id": current_cmid}
                            },
                        }
                    }
                ]
            }

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        previous_group_id = os.environ.get("VK_GROUP_ID")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        os.environ["VK_GROUP_ID"] = "225075265"
        try:
            with (
                patch.object(module, "_vk_owned_run", return_value=row),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(
                    module,
                    "_persist_vk_event_result",
                    side_effect=lambda _row, response, **_kwargs: persisted.append(dict(response)),
                ),
            ):
                result = await module._restore_vk_course_pin_manual(2000000017, dry_run=False)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token
            if previous_group_id is None:
                os.environ.pop("VK_GROUP_ID", None)
            else:
                os.environ["VK_GROUP_ID"] = previous_group_id

        assert result["status"] == "restored"
        assert calls[1] == ("messages.pin", {"peer_id": 2000000017, "cmid": 4})
        assert persisted[-1]["pin_watchdog"]["suspended_by_admin"] is False
        assert persisted[-1]["pin_watchdog"]["trigger"] == "manual"

    asyncio.run(scenario())


def test_manual_pin_restore_upgrades_legacy_split_photo_and_text() -> None:
    async def scenario() -> None:
        calls: list[tuple[str, dict]] = []
        persisted: list[dict] = []
        row = {
            "id": 40,
            "status": "ok",
            "error": "",
            "test_mode": 0,
            "stream_number": "999",
            "date_start": "28.07.2026",
            "course_key": "puppy",
            "request": {"stream_number": "999", "date_start": "28.07.2026", "course_type": "puppy"},
            "response": {
                "peer_id": 2000000017,
                "welcome_photo_cmid": 3,
                "welcome_cmid": 4,
                "pin_watchdog": {"suspended_by_admin": True, "restored_count": 2},
            },
        }

        async def vk_method(method: str, params: dict, _token: str):
            calls.append((method, dict(params)))
            if method == "messages.send":
                return {"message_id": 81, "conversation_message_id": 9}
            if method == "messages.pin":
                return 1
            current_cmid = 4 if len(calls) == 1 else 9
            return {
                "items": [
                    {
                        "conversation": {
                            "peer": {"id": 2000000017},
                            "chat_settings": {
                                "pinned_message": {"conversation_message_id": current_cmid}
                            },
                        }
                    }
                ]
            }

        previous_token = os.environ.get("VK_GROUP_TOKEN")
        previous_group_id = os.environ.get("VK_GROUP_ID")
        os.environ["VK_GROUP_TOKEN"] = "group-token"
        os.environ["VK_GROUP_ID"] = "225075265"
        try:
            with (
                patch.object(module, "_vk_owned_run", return_value=row),
                patch.object(module, "_course_by_input", return_value={"key": "puppy"}),
                patch.object(
                    module,
                    "_selected_people",
                    return_value={"admins": [], "authors": [], "kurators": [], "techs": []},
                ),
                patch.object(module, "_render_template", return_value="Добро пожаловать"),
                patch.object(module, "_asset_path", return_value=module.Path("welcome.jpg")),
                patch.object(module, "_upload_vk_message_photo", return_value="photo-1_2"),
                patch.object(module, "_vk_method", side_effect=vk_method),
                patch.object(
                    module,
                    "_persist_vk_event_result",
                    side_effect=lambda _row, response, **_kwargs: persisted.append(dict(response)),
                ),
            ):
                result = await module._restore_vk_course_pin_manual(2000000017, dry_run=False)
        finally:
            if previous_token is None:
                os.environ.pop("VK_GROUP_TOKEN", None)
            else:
                os.environ["VK_GROUP_TOKEN"] = previous_token
            if previous_group_id is None:
                os.environ.pop("VK_GROUP_ID", None)
            else:
                os.environ["VK_GROUP_ID"] = previous_group_id

        assert result["status"] == "photo_added"
        assert result["target_cmid"] == 9
        send_call = next(params for method, params in calls if method == "messages.send")
        assert send_call["message"] == "Добро пожаловать"
        assert send_call["attachment"] == "photo-1_2"
        assert ("messages.pin", {"peer_id": 2000000017, "cmid": 9}) in calls
        assert persisted[-1]["welcome_cmid"] == 9
        assert persisted[-1]["welcome_photo_cmid"] == 9
        assert persisted[-1]["welcome_message_has_photo"] is True

    asyncio.run(scenario())


def test_vk_invite_fallback_default_points_to_tech_support() -> None:
    assert "https://vk.me/tehpod_sobakovodpro" in module.VK_INVITE_FALLBACK_TEMPLATE
