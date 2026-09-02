from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("_nexus_test_bizon_amocrm_staff", ROOT / "router.py")
module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(module)


def run(value):
    return asyncio.run(value)


async def _seed(module_path: Path) -> tuple[int, int]:
    module._db_path = module_path
    await module._init_db()
    async with module.aiosqlite.connect(module._must_db()) as db:
        first = await db.execute(
            "INSERT INTO bindings(name,priority,responsible_user_ids_json,active) VALUES(?,1,?,1)",
            ("Первый эфир", json.dumps(["10"])),
        )
        second = await db.execute(
            "INSERT INTO bindings(name,priority,responsible_user_ids_json,active) VALUES(?,2,?,1)",
            ("Второй эфир", json.dumps(["20", "30"])),
        )
        await db.commit()
        return int(first.lastrowid), int(second.lastrowid)


def test_staff_connector_reconciles_binding_pools_by_exact_id(tmp_path):
    first_id, second_id = run(_seed(tmp_path / "bizon-amocrm.db"))
    employee = {"identities": [{"provider": "amocrm", "external_id": "20"}]}
    result = run(module.service_staff_apply(
        employee=employee,
        config={"responsible_binding_ids": [str(first_id)]},
        operation="upsert",
    ))
    assert result["changed"] is True
    assert result["config"] == {"responsible_binding_ids": [first_id]}

    async def pools():
        async with module.aiosqlite.connect(module._must_db()) as db:
            return [json.loads(row[0]) for row in await (await db.execute(
                "SELECT responsible_user_ids_json FROM bindings ORDER BY priority,id"
            )).fetchall()]

    assert run(pools()) == [["10", "20"], ["30"]]
    replay = run(module.service_staff_apply(
        employee=employee,
        config={"responsible_binding_ids": [first_id]},
        operation="upsert",
    ))
    assert replay["changed"] is False
    deactivated = run(module.service_staff_apply(
        employee=employee,
        config={},
        operation="deactivate",
    ))
    assert deactivated["snapshot"]["status"] == "unlinked"
    assert run(pools()) == [["10"], ["30"]]
    assert second_id not in deactivated["config"]["responsible_binding_ids"]


def test_staff_connector_validates_before_transactional_write(tmp_path):
    first_id, _ = run(_seed(tmp_path / "bizon-amocrm.db"))
    employee = {"identities": [{"provider": "amocrm", "external_id": "20"}]}
    try:
        run(module.service_staff_apply(
            employee=employee,
            config={"responsible_binding_ids": [first_id, 999999]},
            operation="upsert",
        ))
    except ValueError as exc:
        assert "не найдены" in str(exc)
    else:
        raise AssertionError("unknown binding must fail")

    snapshot = run(module.service_staff_snapshot(employee=employee))
    assert snapshot["config"] if snapshot.get("config") else snapshot["snapshot"]["config"]
    assert snapshot["snapshot"]["config"]["responsible_binding_ids"] != [first_id]


def test_staff_connector_exposes_dynamic_binding_options(tmp_path):
    first_id, second_id = run(_seed(tmp_path / "bizon-amocrm.db"))
    descriptor = run(module.service_staff_connector())
    options = descriptor["fields"][0]["options"]
    assert [option["value"] for option in options] == [str(first_id), str(second_id)]
