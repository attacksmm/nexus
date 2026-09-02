import sqlite3

from module_course_chat_creator import router as module


def _use_temp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "course-chat-creator.db"
    monkeypatch.setattr(module, "_db_path", lambda: db_path)
    monkeypatch.setattr(module, "_db_initialized", False)
    module._init_db()
    return db_path


def test_staff_connector_upserts_by_exact_identity_and_deactivates_without_delete(tmp_path, monkeypatch):
    db_path = _use_temp_db(tmp_path, monkeypatch)
    employee = {
        "id": "employee-course-1",
        "full_name": "Мария Реестрова",
        "display_name": "Мария",
        "roles": ["curator"],
        "identities": [
            {"provider": "vk", "external_id": "99112233"},
            {"provider": "telegram", "username": "registry_maria"},
        ],
        "source_links": {},
    }
    created = module.service_staff_apply(
        employee=employee,
        config={"kind": "kurator", "offer_id": 880011, "parity": "odd", "note": "Из единого реестра"},
        operation="upsert",
        idempotency_key="course-create-1",
    )
    assert created["ok"] is True
    assert created["changed"] is True
    assert created["config"] == {
        "kind": "kurator",
        "vk_id": "99112233",
        "tg_ref": "@registry_maria",
        "offer_id": 880011,
        "parity": "odd",
        "note": "Из единого реестра",
        "enabled": True,
    }

    updated_employee = {**employee, "display_name": "Мария Р.", "source_links": {}}
    updated = module.service_staff_apply(
        employee=updated_employee,
        config={"kind": "kurator"},
        operation="upsert",
        idempotency_key="course-update-1",
    )
    assert updated["local_id"] == created["local_id"]
    assert updated["snapshot"]["display_name"] == "Мария Р."
    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM people WHERE vk_id='99112233'").fetchone()[0] == 1

    linked_employee = {**employee, "source_links": {module.DEFAULT_MODULE_ID: {"local_id": created["local_id"]}}}
    deactivated = module.service_staff_apply(
        employee=linked_employee,
        config={},
        operation="deactivate",
        idempotency_key="course-deactivate-1",
    )
    assert deactivated["changed"] is True
    assert deactivated["config"]["enabled"] is False
    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT enabled FROM people WHERE id=?", (int(created["local_id"]),)).fetchone() == (0,)


def test_staff_connector_never_fuzzy_matches_name_and_lists_safe_snapshots(tmp_path, monkeypatch):
    _use_temp_db(tmp_path, monkeypatch)
    existing = next(item for item in module.service_staff_list() if item["display_name"] == "Наталья")
    not_linked = module.service_staff_snapshot(employee={
        "id": "different-natalia",
        "display_name": "Наталья",
        "identities": [],
        "source_links": {},
    })
    assert not_linked["found"] is False
    linked = module.service_staff_snapshot(employee={
        "id": "real-natalia",
        "source_links": {module.DEFAULT_MODULE_ID: existing["local_id"]},
        "identities": [],
    })
    assert linked["found"] is True
    assert linked["local_id"] == existing["local_id"]
    assert module.service_staff_connector()["deactivate_preserves_history"] is True
