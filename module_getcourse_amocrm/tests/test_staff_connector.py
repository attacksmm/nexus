from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("_nexus_test_getcourse_amocrm_staff", ROOT / "router.py")
module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(module)


def run(value):
    return asyncio.run(value)


async def _seed(module_path: Path) -> tuple[int, int]:
    module._db_path = module_path
    await module._init_db()
    async with module._connect() as db:
        rows = await (await db.execute("SELECT id FROM bindings ORDER BY id LIMIT 2")).fetchall()
        first_id, second_id = int(rows[0][0]), int(rows[1][0])
        await db.execute(
            "UPDATE settings SET value=? WHERE key='responsible_user_ids_json'",
            (json.dumps(["10", "20"]),),
        )
        await db.execute("UPDATE settings SET value='10' WHERE key='responsible_user_id'")
        await db.execute(
            "UPDATE bindings SET responsible_user_id='20',task_responsible_user_id='30' WHERE id=?",
            (first_id,),
        )
        await db.execute(
            "UPDATE bindings SET responsible_user_id='',task_responsible_user_id='' WHERE id=?",
            (second_id,),
        )
        await db.commit()
    return first_id, second_id


def test_staff_connector_preserves_global_pool_and_reconciles_exact_assignments(tmp_path):
    first_id, second_id = run(_seed(tmp_path / "getcourse-amocrm.db"))
    employee = {"identities": [{"provider": "amocrm", "external_id": "20"}]}
    result = run(module.service_staff_apply(
        employee=employee,
        config={
            "round_robin_enabled": False,
            "deal_binding_ids": [first_id],
            "task_binding_ids": [second_id],
        },
        operation="upsert",
    ))
    assert result["config"] == {
        "round_robin_enabled": False,
        "deal_binding_ids": [first_id],
        "task_binding_ids": [second_id],
    }

    async def state():
        async with module._connect() as db:
            pool = await (await db.execute(
                "SELECT value FROM settings WHERE key='responsible_user_ids_json'"
            )).fetchone()
            rows = await (await db.execute(
                "SELECT id,responsible_user_id,task_responsible_user_id FROM bindings WHERE id IN (?,?) ORDER BY id",
                (first_id, second_id),
            )).fetchall()
        return json.loads(pool[0]), [tuple(row) for row in rows]

    assert run(state()) == (["10"], [(first_id, "20", "30"), (second_id, "", "20")])
    deactivated = run(module.service_staff_apply(employee=employee, config={}, operation="deactivate"))
    assert deactivated["snapshot"]["status"] == "unlinked"
    assert run(state()) == (["10"], [(first_id, "", "30"), (second_id, "", "")])


def test_staff_connector_conflict_rolls_back_every_field(tmp_path):
    first_id, second_id = run(_seed(tmp_path / "getcourse-amocrm.db"))
    employee = {"identities": [{"provider": "amocrm", "external_id": "20"}]}
    try:
        run(module.service_staff_apply(
            employee=employee,
            config={
                "round_robin_enabled": False,
                "deal_binding_ids": [second_id],
                "task_binding_ids": [],
            },
            operation="upsert",
        ))
    except ValueError as exc:
        # Reserve the target for a different employee and retry below.
        assert False, str(exc)

    async def reserve_and_read():
        async with module._connect() as db:
            await db.execute("UPDATE bindings SET responsible_user_id='99' WHERE id=?", (first_id,))
            await db.execute("UPDATE settings SET value=? WHERE key='responsible_user_ids_json'", (json.dumps(["10", "20"]),))
            await db.commit()
        try:
            await module.service_staff_apply(
                employee=employee,
                config={"round_robin_enabled": False, "deal_binding_ids": [first_id], "task_binding_ids": []},
                operation="upsert",
            )
        except ValueError as exc:
            assert "другому" in str(exc)
        else:
            raise AssertionError("assignment conflict must fail")
        async with module._connect() as db:
            pool = await (await db.execute("SELECT value FROM settings WHERE key='responsible_user_ids_json'")).fetchone()
            owner = await (await db.execute("SELECT responsible_user_id FROM bindings WHERE id=?", (first_id,))).fetchone()
        return json.loads(pool[0]), owner[0]

    assert run(reserve_and_read()) == (["10", "20"], "99")


def test_staff_connector_exposes_dynamic_binding_options(tmp_path):
    first_id, second_id = run(_seed(tmp_path / "getcourse-amocrm.db"))
    descriptor = run(module.service_staff_connector())
    fields = {field["key"]: field for field in descriptor["fields"]}
    assert fields["round_robin_enabled"]["type"] == "bool"
    assert [option["value"] for option in fields["deal_binding_ids"]["options"]][:2] == [str(first_id), str(second_id)]
