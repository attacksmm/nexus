import sqlite3
import time

from module_chat_moderators import router as module


def test_module_zero_preserves_intro_group(monkeypatch):
    catalog = [
        {"group_id": 1, "name": "Знакомство. Щенок", "managed": True, "course_key": "puppy", "group_kind": "root"},
        {"group_id": 2, "name": "0 модуль. Щенок", "managed": True, "course_key": "puppy", "group_kind": "module", "module_index": 0},
        {"group_id": 3, "name": "3 модуль. Щенок", "managed": True, "course_key": "puppy", "group_kind": "module", "module_index": 3},
        {"group_id": 4, "name": "Премиум. Щенок", "managed": True, "course_key": "puppy", "group_kind": "package", "package_key": "premium"},
    ]
    monkeypatch.setattr(module.NexusGetCourseAccessService, "_catalog", lambda _self: catalog)
    monkeypatch.setattr(module, "_gc_create_group_backup", lambda **_kwargs: 1)
    monkeypatch.setattr(module, "_gc_create_access_request", lambda **_kwargs: None)

    current = [catalog[3]]
    enabled = module.service_prepare_access_change(
        gc_user_id="1", email="a@example.com", current_groups=current,
        changes=[{"group_id": "2", "enabled": True}], requester_user_id="1",
    )
    assert {item["name"] for item in enabled["target_groups"]} == {
        "Премиум. Щенок", "0 модуль. Щенок"
    }

    unchanged = module.service_prepare_access_change(
        gc_user_id="1", email="a@example.com", current_groups=[catalog[0], catalog[2], catalog[3]],
        changes=[{"group_id": "3", "enabled": False}], requester_user_id="1",
    )
    assert {item["name"] for item in unchanged["target_groups"]} == {
        "Знакомство. Щенок", "Премиум. Щенок"
    }

    disabled = module.service_prepare_access_change(
        gc_user_id="1", email="a@example.com", current_groups=[catalog[0], catalog[1], catalog[3]],
        changes=[{"group_id": "2", "enabled": False}], requester_user_id="1",
    )
    assert {item["name"] for item in disabled["target_groups"]} == {"Знакомство. Щенок", "Премиум. Щенок"}


def test_access_verification_queue_persists_without_schema_change(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setattr(module, "_data_dir", lambda: data)
    with sqlite3.connect(data / "getcourse_access.db") as db:
        db.execute(
            """
            CREATE TABLE gc_access_requests(
                request_id TEXT PRIMARY KEY, requester_chat_id TEXT, requester_user_id TEXT,
                command_text TEXT, identifier TEXT, gc_user_id TEXT, parsed_json TEXT,
                current_groups_json TEXT, target_groups_json TEXT, backup_id INTEGER,
                status TEXT, preview_text TEXT, created_at REAL, applied_at REAL,
                apply_result_json TEXT
            )
            """
        )
        db.execute(
            "INSERT INTO gc_access_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "req1", "streams", "1", "streams-access", "user@example.com", "100", "{}",
                '[{"group_id":1,"name":"Курс"}]',
                '[{"group_id":1,"name":"Курс"},{"group_id":2,"name":"2 модуль"}]',
                None, "applied", "", time.time() - 10, time.time() - 5, "{}",
            ),
        )
        for index in range(120):
            db.execute(
                "INSERT INTO gc_access_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"old{index}", "streams", "1", "streams-access", f"old{index}@example.com", str(index), "{}",
                    "[]", "[]", None, "applied", "", time.time() - 1000 + index,
                    time.time() - 1000 + index, '{"verification_pending":false}',
                ),
            )

    scheduled = module.service_schedule_access_verification(request_id="req1", delay_seconds=15)
    assert scheduled["next_check_at"]
    assert module.service_latest_access_verification(gc_user_id="100")["pending"] is True

    with module._gc_db() as db:
        result = module._gc_json_loads(db.execute("SELECT apply_result_json FROM gc_access_requests WHERE request_id='req1'").fetchone()[0], {})
        result["verification_next_at"] = time.time() - 1
        db.execute(
            "UPDATE gc_access_requests SET apply_result_json=? WHERE request_id='req1'",
            (module._gc_json_dumps(result),),
        )
    assert module.service_pending_access_verifications(limit=1)["items"][0]["request_id"] == "req1"

    verified = module.service_record_access_verification(
        request_id="req1",
        actual_groups=[{"group_id": 1, "name": "Курс"}, {"group_id": 2, "name": "2 модуль"}],
        defer_on_mismatch=True,
    )
    assert verified["verified"] is True
    assert module.service_latest_access_verification(gc_user_id="100")["pending"] is False
