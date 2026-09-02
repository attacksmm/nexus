import asyncio
import sqlite3
import sys
from types import SimpleNamespace
from pathlib import Path

from module_getcourse_chat_fields import router as module


def run(coro):
    return asyncio.run(coro)


def test_staff_connector_updates_only_linked_curator_mapping(tmp_path):
    db_path = tmp_path / "chat-fields.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.execute(
            "INSERT INTO settings(key,value) VALUES('curator_map',?)",
            ("ирина=Куратор 1;слава=Куратор 2",),
        )
    old_path = module._db_path
    module._db_path = db_path
    employee = {
        "full_name": "Ирина Новая",
        "source_links": {module.MODULE_ID: {"local_id": "ирина"}},
    }
    try:
        result = run(module.service_staff_apply(
            employee=employee,
            config={"name_markers": ["Ирина", "Ира"], "curator_value": "Куратор 7"},
            operation="upsert",
            idempotency_key="test",
        ))
        settings = run(module._settings_map())
        assert result["local_id"] == "ирина"
        assert module._curator_name_map(settings) == (
            ("слава", "Куратор 2"), ("ирина", "Куратор 7"), ("ира", "Куратор 7")
        )
        run(module.service_staff_apply(
            employee=employee,
            config={"name_markers": ["Ирина", "Ира"], "curator_value": "Куратор 7"},
            operation="deactivate",
        ))
        settings = run(module._settings_map())
        assert ("слава", "Куратор 2") in module._curator_name_map(settings)
        assert not any(value == "Куратор 7" for _, value in module._curator_name_map(settings))
    finally:
        module._db_path = old_path


def test_panel_delegates_curator_people_to_staff_registry():
    html = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text("utf-8")
    assert "/nexus/staff-registry/panel/" in html
    assert 'id="curator_map"' not in html
    assert "Сохраняю настройки" in html
    assert "class=\"spinner\"" in html


def test_legacy_settings_save_preserves_explicit_empty_registry_mapping(tmp_path, monkeypatch):
    db_path = tmp_path / "chat-fields.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.execute("INSERT INTO settings(key,value) VALUES('curator_map','')")
    monkeypatch.setattr(module, "_db_path", db_path)
    monkeypatch.setitem(sys.modules, "_nexus_mod_staff-registry", SimpleNamespace())

    async def allow(_request):
        return {"username": "admin"}

    captured = {}

    async def save(values):
        captured.update(values)
        return values

    async def view(values):
        return values

    class Request:
        async def json(self):
            return {"curator_map": "локальная=подмена", "curator_cell": "A1"}

    monkeypatch.setattr(module, "_require_user", allow)
    monkeypatch.setattr(module, "_save_settings", save)
    monkeypatch.setattr(module, "get_settings_from_map", view)
    run(module.post_settings(Request()))
    assert captured["curator_map"] == ""
