from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("_nexus_test_admin_handoff_staff", ROOT / "router.py")
module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(module)


def run(value):
    return asyncio.run(value)


def test_staff_connector_updates_only_exact_vk_id_and_preserves_filter(tmp_path):
    module._db_path = tmp_path / "admin-handoff.db"
    run(module._init_db())

    async def seed():
        async with module._connect() as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('admin_filter_enabled','1')"
            )
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('allowed_admin_ids',?)",
                (json.dumps(["100", "1000"]),),
            )
            await db.commit()

    run(seed())
    employee = {"identities": [{"provider": "vk", "external_id": "100"}]}
    removed = run(module.service_staff_apply(
        employee=employee,
        config={"protect_dialogs": False},
        operation="upsert",
        idempotency_key="remove-100",
    ))
    assert removed["changed"] is True
    assert removed["snapshot"]["config"]["protect_dialogs"] is False
    assert run(module._allowed_admin_ids()) == ["1000"]
    assert run(module._setting("admin_filter_enabled")) == "1"

    restored = run(module.service_staff_apply(
        employee=employee,
        config={"protect_dialogs": True},
        operation="upsert",
        idempotency_key="restore-100",
    ))
    assert restored["snapshot"]["active"] is True
    assert run(module._allowed_admin_ids()) == ["100", "1000"]
    replay = run(module.service_staff_apply(
        employee=employee,
        config={"protect_dialogs": True},
        operation="upsert",
        idempotency_key="restore-100-again",
    ))
    assert replay["changed"] is False


def test_staff_connector_rejects_conflicting_exact_vk_id(tmp_path):
    module._db_path = tmp_path / "admin-handoff.db"
    run(module._init_db())
    employee = {
        "source_links": {module.MODULE_ID: "100"},
        "identities": [{"provider": "vk", "external_id": "101"}],
    }
    try:
        run(module.service_staff_apply(employee=employee, config={}, operation="upsert"))
    except ValueError as exc:
        assert "разных" in str(exc)
    else:
        raise AssertionError("conflicting VK identities must fail")


def test_staff_connector_descriptor_uses_registry_field_name():
    descriptor = module.service_staff_connector()
    assert descriptor["identity"]["provider"] == "vk"
    assert [field["key"] for field in descriptor["fields"]] == ["protect_dialogs"]


def test_first_registry_write_seeds_legacy_allow_all_roster(tmp_path):
    module._db_path = tmp_path / "admin-handoff.db"
    run(module._init_db())
    original = module._admins_payload

    async def observed(*_args, **_kwargs):
        return {
            "items": [{"id": "100", "name": "A"}, {"id": "200", "name": "B"}],
            "allowed_admin_ids": [], "admin_filter_enabled": False,
        }

    module._admins_payload = observed
    try:
        run(module.service_staff_apply(
            employee={"identities": [{"provider": "vk", "external_id": "100"}]},
            config={"protect_dialogs": False}, operation="upsert",
        ))
    finally:
        module._admins_payload = original
    assert run(module._allowed_admin_ids()) == ["200"]
    assert run(module._setting("admin_filter_enabled")) == "1"
