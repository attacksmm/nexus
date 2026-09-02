import asyncio
import json
import re
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from module_staff_registry import router as registry


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def ready(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "_db_path", tmp_path / "staff-registry.db")
    monkeypatch.setattr(registry, "_worker_task", None)
    run(registry._init_db())
    return registry


def test_employee_is_centralized_and_secrets_are_never_persisted(ready):
    employee, jobs = run(ready._save_employee({
        "full_name": "Мария Реестрова",
        "display_name": "Мария",
        "job_profile": "custom",
        "status": "active",
        "roles": ["curator", {"role": "support", "scope": "school"}],
        "identities": [{"provider": "telegram", "external_id": "9911", "username": "maria"}],
        "modules": {
            "student-transfer": {
                "enabled": True,
                "config": {"login": "maria", "password": "must-not-be-stored", "token": "also-secret"},
            }
        },
    }, "admin"))
    assert employee["status"] == "active"
    assert employee["memberships"][0]["config"] == {"login": "maria"}
    assert employee["source_links"] == {}
    assert len(jobs) == 1
    raw = ready._must_db().read_bytes()
    assert b"must-not-be-stored" not in raw
    assert b"also-secret" not in raw


def test_exact_identity_cannot_belong_to_two_people(ready):
    body = {
        "full_name": "Первый сотрудник", "job_profile": "custom",
        "identities": [{"provider": "vk", "external_id": "42"}], "modules": {},
    }
    run(ready._save_employee(body, "admin"))
    with pytest.raises(Exception) as error:
        run(ready._save_employee({**body, "full_name": "Второй сотрудник"}, "admin"))
    assert getattr(error.value, "status_code", 0) == 409


def test_stale_upsert_never_reenables_offboarded_employee(ready, monkeypatch):
    calls = []

    async def apply(**kwargs):
        calls.append(kwargs["operation"])
        return {"ok": True, "local_id": "55", "changed": True}

    fake_module = SimpleNamespace(service_staff_apply=apply)
    monkeypatch.setitem(sys.modules, "_nexus_mod_sales-chats", fake_module)
    employee, jobs = run(ready._save_employee({
        "full_name": "Иван Менеджер", "job_profile": "custom", "status": "active",
        "modules": {"sales-chats": {"enabled": True, "config": {"login": "ivan"}}},
    }, "admin"))
    employee_id = employee["id"]

    async def scenario():
        db = await ready._connect()
        try:
            stale = dict(await (await db.execute("SELECT * FROM sync_jobs WHERE id=?", (jobs[0],))).fetchone())
            await db.execute("UPDATE employees SET status='offboarded',version=version+1 WHERE id=?", (employee_id,))
            await db.commit()
        finally:
            await db.close()
        await ready._apply_job(stale)
        detail = await ready._employee(employee_id)
        db = await ready._connect()
        try:
            job = dict(await (await db.execute("SELECT * FROM sync_jobs WHERE id=?", (jobs[0],))).fetchone())
        finally:
            await db.close()
        return detail, job

    detail, job = run(scenario())
    assert calls == []
    assert job["status"] == "done"
    assert json.loads(job["result_json"])["superseded"] is True
    assert detail["memberships"][0]["sync_status"] == "pending"


def test_discovery_matches_only_exact_identity_not_same_name(ready, monkeypatch):
    existing, _ = run(ready._save_employee({
        "full_name": "Наталья", "job_profile": "custom",
        "identities": [{"provider": "vk", "external_id": "100"}], "modules": {},
    }, "admin"))
    rows = [
        {"local_id": "1", "full_name": "Наталья", "identities": [{"provider": "vk", "external_id": "999"}]},
        {"local_id": "2", "full_name": "Другое имя", "identities": [{"provider": "vk", "external_id": "100"}]},
    ]
    monkeypatch.setitem(sys.modules, "_nexus_mod_course-chat-creator", SimpleNamespace(service_staff_list=lambda: rows))
    monkeypatch.setattr(ready, "MODULE_DEFINITIONS", {"course-chat-creator": ready.MODULE_DEFINITIONS["course-chat-creator"]})
    monkeypatch.setattr(ready, "get_all_users", lambda: asyncio.sleep(0, result=[]))
    run(ready._discover_candidates("admin"))

    async def candidates():
        db = await ready._connect()
        try:
            found = await (await db.execute(
                "SELECT local_id,match_employee_id,match_reason FROM discovery_candidates ORDER BY local_id"
            )).fetchall()
            return [dict(row) for row in found]
        finally:
            await db.close()

    found = run(candidates())
    assert found[0]["match_employee_id"] == ""
    assert found[1]["match_employee_id"] == existing["id"]
    assert found[1]["match_reason"] == "identity:vk"


def test_panel_has_loading_feedback_mobile_layout_and_safe_rendering():
    html = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text()
    assert "@media(max-width:860px)" in html
    assert "@media(max-width:560px)" in html
    assert len(re.findall(r'class=\"spinner\"', html)) >= 10
    assert "Загружаю сотрудников" in html
    assert "Ищу аккаунты" in html
    assert "Сохраняю и применяю" in html
    assert "const esc=" in html
    assert "${esc(p.full_name)}" in html


def test_russian_name_slug_is_stable_and_ascii():
    assert registry._slug("Мария Реестрова") == "mariya.reestrova"


def test_registry_covers_every_employee_management_zone():
    expected = {
        "nexus-core", "messenger-widget", "course-chat-creator", "student-transfer",
        "sales-chats", "sbkvd-gpt", "email-channel", "admin-handoff",
        "chat-moderator", "chat-moderators", "bizon-amocrm", "getcourse-amocrm",
        "getcourse-chat-fields",
    }
    assert expected <= set(registry.MODULE_DEFINITIONS)


def test_panel_supports_dynamic_assignment_multiselects():
    html = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text("utf-8")
    assert "f.type==='multiselect'" in html
    assert "optionParts" in html
    assert "input:checked" in html
    assert "config={...(state.editModules[id]?.config||{})}" in html
    assert "messenger-activation-code" in html
    assert "messenger-devices" in html


def test_legacy_panels_delegate_employee_management_to_one_hub():
    root = Path(__file__).resolve().parents[2]
    panels = [
        "module_messenger_widget/panel/index.html",
        "module_course_chat_creator/panel/index.html",
        "module_student_transfer/panel/index.html",
        "module_email_channel/panel/index.html",
        "module_sales_chats/panel/index.html",
        "module_sbkvd_gpt/panel/index.html",
        "module_admin_handoff/panel/index.html",
        "module_chat_moderator/panel/index.html",
        "module_chat_moderators/panel/index.html",
        "module_bizon_amocrm/panel/index.html",
        "module_getcourse_amocrm/panel/index.html",
        "module_getcourse_chat_fields/panel/index.html",
    ]
    for relative in panels:
        html = (root / relative).read_text("utf-8")
        assert "/nexus/staff-registry/panel/" in html, relative

    messenger = (root / panels[0]).read_text("utf-8")
    for hidden_staff_tab in ('data-view="admins"', 'data-view="routing"', 'data-view="devices"'):
        assert hidden_staff_tab not in messenger.split("</nav>", 1)[0]

    for relative in (panels[7], panels[8]):
        html = (root / relative).read_text("utf-8")
        assert 'id="tgAllowed"' not in html
        assert 'id="vkAdmins"' not in html
        assert 'id="vkTrusted"' not in html
