import json
import sqlite3

from module_course_chat_creator import router as module


def test_flow_catalog_uses_native_clean_contract(tmp_path, monkeypatch):
    db_path = tmp_path / "course-chat-creator.db"
    monkeypatch.setattr(module, "_db_path", lambda: db_path)
    monkeypatch.setattr(module, "_db_initialized", False)
    module._init_db()

    setup = module.service_flow_setup()
    catalog = module.service_flow_catalog()

    assert catalog["ok"] is True
    assert setup["teachers"]
    assert 8593080 in {item["offer_id"] for item in setup["teachers"]}


def test_transfer_readiness_is_scoped_to_exact_course_and_stream(tmp_path, monkeypatch):
    db_path = tmp_path / "course-chat-creator.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY, platform TEXT, title TEXT, stream_number TEXT,
                date_start TEXT, course_key TEXT, test_mode INTEGER, status TEXT,
                link TEXT, chat_id TEXT, error TEXT, request_json TEXT,
                response_json TEXT, created_at INTEGER
            )
            """
        )
        db.executemany(
            "INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "vk", "55. 01.01.2026 - Курс Щенок. Современный Собаковод", "55", "", "puppy", 0, "ok", "", "1", "", "{}", json.dumps({"peer_id": 2000000001, "owner_group_id": 0}), 1),
                (2, "telegram", "55. 01.01.2026 - Курс Щенок. Современный Собаковод", "55", "", "puppy", 0, "ok", "", "101", "", "{}", "{}", 2),
                (3, "vk", "56. 01.01.2026 - Курс Щенок. Современный Собаковод", "56", "", "puppy", 0, "ok", "", "2", "", "{}", json.dumps({"peer_id": 2000000002, "owner_group_id": 123}), 3),
            ],
        )
    monkeypatch.setattr(module, "_db_path", lambda: db_path)
    monkeypatch.setattr(module, "_db_initialized", True)
    monkeypatch.setenv("VK_GROUP_ID", "123")

    legacy = module.service_transfer_chat_readiness("puppy", "55")
    ready = module.service_transfer_chat_readiness("puppy", "56")

    assert legacy["vk"]["status"] == "legacy_inaccessible"
    assert legacy["telegram"]["status"] == "ready"
    assert ready["vk"]["status"] == "ready"
    assert ready["telegram"]["status"] == "not_recorded"
