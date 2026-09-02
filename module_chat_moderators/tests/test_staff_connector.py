import asyncio
import json
import sqlite3
from types import SimpleNamespace

import pytest

from module_chat_moderators import router as module


def _ready(tmp_path, monkeypatch):
    db_path = tmp_path / "chat-moderators.db"
    monkeypatch.setattr(module, "_db_path", lambda: db_path)
    monkeypatch.setattr(module, "_runtime", None)
    module._init_db()
    module._save_settings({
        "vk_allowed_admins": "101,202",
        "vk_trusted_senders": "202",
        "tg_allowed_adders": "505",
    })
    return db_path


def _employee(vk_id, *, source_id=None, name="Мария"):
    links = {} if source_id is None else {module.MODULE_ID: {"local_id": str(source_id)}}
    return {
        "id": "employee-1",
        "full_name": name,
        "display_name": name,
        "identities": [{"provider": "vk", "external_id": str(vk_id)}],
        "source_links": links,
    }


def test_staff_connector_updates_exact_vk_id_and_deactivation_removes_it(tmp_path, monkeypatch):
    _ready(tmp_path, monkeypatch)
    employee = _employee(303)

    created = module.service_staff_apply(
        employee=employee,
        config={"allowed_admin": True, "trusted_sender": False},
        operation="upsert",
    )
    assert created["changed"] is True
    assert created["config"] == {"allowed_admin": True, "trusted_sender": False, "telegram_allowed_adder": False}
    settings = module._settings()
    assert module._int_set_csv(settings["vk_allowed_admins"]) == {101, 202, 303}
    assert module._int_set_csv(settings["vk_trusted_senders"]) == {202}

    moved = module.service_staff_apply(
        employee=_employee(404, source_id=303),
        config={"trusted_sender": True},
        operation="upsert",
    )
    assert moved["config"] == {"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": False}
    settings = module._settings()
    assert 303 not in module._int_set_csv(settings["vk_allowed_admins"])
    assert 303 not in module._int_set_csv(settings["vk_trusted_senders"])
    assert 404 in module._int_set_csv(settings["vk_allowed_admins"])
    assert 404 in module._int_set_csv(settings["vk_trusted_senders"])

    same_name_other_vk = module.service_staff_snapshot(employee=_employee(330, name="Мария"))
    assert same_name_other_vk["found"] is False
    exported = next(item for item in module.service_staff_list() if item["local_id"] == "404")
    assert exported["identities"] == [{"provider": "vk", "external_id": "404"}]

    deactivated = module.service_staff_apply(
        employee=_employee(404, source_id=404), config={}, operation="deactivate",
    )
    assert deactivated["changed"] is True
    assert deactivated["config"] == {"allowed_admin": False, "trusted_sender": False, "telegram_allowed_adder": False}
    settings = module._settings()
    assert 404 not in module._int_set_csv(settings["vk_allowed_admins"])
    assert 404 not in module._int_set_csv(settings["vk_trusted_senders"])


def test_staff_connector_write_is_atomic(tmp_path, monkeypatch):
    db_path = _ready(tmp_path, monkeypatch)
    staff_keys = (*module._STAFF_VK_SETTING_KEYS, module._STAFF_TG_SETTING_KEY)
    before = {key: module._settings()[key] for key in staff_keys}
    with sqlite3.connect(db_path) as db:
        db.executescript(
            """
            CREATE TRIGGER reject_telegram_staff_update
            BEFORE UPDATE ON settings
            WHEN NEW.key='tg_allowed_adders' AND instr(NEW.value,'707') > 0
            BEGIN
                SELECT RAISE(ABORT, 'synthetic second-setting failure');
            END;
            """
        )
    with pytest.raises(sqlite3.IntegrityError, match="synthetic second-setting failure"):
        module.service_staff_apply(
            employee={
                **_employee(303),
                "identities": [
                    {"provider": "vk", "external_id": "303"},
                    {"provider": "telegram", "external_id": "707"},
                ],
            },
            config={"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
            operation="upsert",
        )
    assert {key: module._settings()[key] for key in staff_keys} == before


def test_shared_course_registry_remains_unmanaged_and_effective(tmp_path, monkeypatch):
    _ready(tmp_path, monkeypatch)
    shared_path = tmp_path / "course-chat-creator.db"
    payload = {"user_ids": [909], "source_peer_ids": [2000000092]}
    with sqlite3.connect(shared_path) as db:
        db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        db.execute(
            "INSERT INTO meta(key,value) VALUES('vk_staff_registry_v1',?)",
            (json.dumps(payload),),
        )
    monkeypatch.setattr(module, "_course_chat_creator_db_path", lambda: shared_path)
    module._vk_staff_registry_cache.clear()
    assert module._trusted_vk_staff_ids() == {909}

    result = module.service_staff_apply(employee=_employee(909), config={}, operation="deactivate")
    assert result["changed"] is False
    module._vk_staff_registry_cache.clear()
    assert module._trusted_vk_staff_ids() == {909}
    with sqlite3.connect(shared_path) as db:
        stored = json.loads(db.execute(
            "SELECT value FROM meta WHERE key='vk_staff_registry_v1'"
        ).fetchone()[0])
    assert stored == payload

    descriptor = module.service_staff_connector()
    assert set(descriptor["config_schema"]) == {"allowed_admin", "trusted_sender", "telegram_allowed_adder"}
    capability = descriptor["unmanaged_capabilities"][0]
    assert capability == {
        "key": "course_chat_creator_vk_staff",
        "source_module": "course-chat-creator",
        "managed": False,
        "read_only": True,
        "effects": ["auto_promote_vk_chat_admin", "protect_from_moderation"],
    }


def test_staff_connector_rejects_non_exact_vk_identity(tmp_path, monkeypatch):
    _ready(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="точный положительный numeric ID"):
        module.service_staff_apply(
            employee=_employee("vk.com/maria"),
            config={"allowed_admin": True, "trusted_sender": True},
            operation="upsert",
        )


def test_staff_connector_manages_exact_telegram_adder_and_panel_has_no_local_editor(tmp_path, monkeypatch):
    _ready(tmp_path, monkeypatch)
    employee = {"identities": [{"provider": "telegram", "external_id": "707"}], "source_links": {}}
    result = module.service_staff_apply(
        employee=employee, config={"telegram_allowed_adder": True}, operation="upsert",
    )
    assert result["local_id"] == "tg:707"
    assert 707 in module._int_set_csv(module._settings()["tg_allowed_adders"])
    module.service_staff_apply(employee=employee, config={}, operation="deactivate")
    assert 707 not in module._int_set_csv(module._settings()["tg_allowed_adders"])
    html = (module.Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text("utf-8")
    assert "/nexus/staff-registry/panel/" in html
    assert 'id="tgAllowed"' not in html and 'id="vkAdmins"' not in html and 'id="vkTrusted"' not in html


def test_staff_apply_refreshes_running_vk_and_telegram_settings(tmp_path, monkeypatch):
    _ready(tmp_path, monkeypatch)
    runtime = SimpleNamespace(
        vk=SimpleNamespace(settings={"stale": "vk"}),
        telegram=SimpleNamespace(settings={"stale": "telegram"}),
    )
    monkeypatch.setattr(module, "_runtime", runtime)
    employee = {
        "identities": [
            {"provider": "vk", "external_id": "303"},
            {"provider": "telegram", "external_id": "707"},
        ],
        "source_links": {},
    }
    module.service_staff_apply(
        employee=employee,
        config={"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
        operation="upsert",
    )
    assert runtime.vk.settings == module._settings()
    assert runtime.telegram.settings == module._settings()
    assert 707 in module._int_set_csv(runtime.telegram.settings["tg_allowed_adders"])


def test_running_telegram_runtime_accepts_live_settings_refresh():
    async def scenario():
        runtime = module.TelegramModeratorRuntime(module.ModerationAnalyzer())
        runtime.running = True
        await runtime.start({"tg_allowed_adders": "707"})
        assert runtime.settings["tg_allowed_adders"] == "707"
    asyncio.run(scenario())
