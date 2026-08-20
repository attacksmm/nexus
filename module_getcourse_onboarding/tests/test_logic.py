import asyncio
import inspect
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import router


class OnboardingLogicTests(unittest.TestCase):
    def test_completed_monitor_alerts_only_after_three_consecutive_export_delays(self):
        job = {"status": "completed", "attempts": 0}
        self.assertTrue(router._completed_monitor_transient("Файл ещё не создан"))
        self.assertTrue(router._completed_monitor_transient("Лимит GetCourse Export API: live-проверка отложена"))
        self.assertFalse(router._should_alert_upgrade_exception(job, "Файл еще не создан"))
        self.assertFalse(router._should_alert_upgrade_exception({**job, "attempts": 1}, "Файл еще не создан"))
        self.assertTrue(router._should_alert_upgrade_exception({**job, "attempts": 2}, "Файл еще не создан"))
        self.assertTrue(router._should_alert_upgrade_exception(job, "Неизвестная ошибка схемы"))
        self.assertTrue(router._should_alert_upgrade_exception({"status": "validated", "attempts": 0}, "Файл еще не создан"))

    def test_successful_completed_monitor_resets_consecutive_attempts(self):
        async def scenario() -> None:
            previous_db, previous_ready, previous_error = router._db_path, router._db_ready, router._init_error
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    router._db_path = Path(temp_dir) / "onboarding.db"
                    router._db_ready = asyncio.Event()
                    router._init_error = ""
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            """INSERT INTO upgrade_jobs(
                               upgrade_order_id,course_key,status,approved,operation_id,strategy,
                               attempts,created_at,updated_at
                               ) VALUES('surcharge','puppy','completed',1,'op-monitor','replacement_browser',2,?,?)""",
                            (now, now),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    await router._set_upgrade_state(
                        1, "completed", delay_seconds=6 * 60 * 60, reset_attempts=True,
                    )
                    db = await router._connect()
                    try:
                        row = await (await db.execute(
                            "SELECT status,error,attempts,next_attempt_at FROM upgrade_jobs WHERE id=1"
                        )).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["status"], "completed")
                    self.assertEqual(row["error"], "")
                    self.assertEqual(row["attempts"], 0)
                    self.assertTrue(row["next_attempt_at"])
            finally:
                router._db_path, router._db_ready, router._init_error = previous_db, previous_ready, previous_error

        asyncio.run(scenario())

    def test_email_packages_are_paused_by_default_and_vip_deduplicates_with_premium(self):
        self.assertEqual(router.DEFAULT_SETTINGS["email_mode"], "paused")
        self.assertEqual(router.DEFAULT_SETTINGS["email_enabled"], "0")
        self.assertEqual(router.DEFAULT_SETTINGS["email_process_confirmed"], "0")
        self.assertEqual(router._email_package_key("puppy", "premium"), "puppy:premium-entry")
        self.assertEqual(router._email_package_key("puppy", "vip"), "puppy:premium-entry")
        self.assertEqual(router._email_package_key("dog", "standard"), "dog:standard-start")

    def test_each_email_package_has_a_distinct_getcourse_trigger_group(self):
        settings = {"email_trigger_group_template": "Nexus email {package_key}"}
        packages = (
            "puppy:standard-start", "puppy:premium-entry",
            "dog:standard-start", "dog:premium-entry",
            "combo:standard-start", "combo:premium-entry",
        )
        groups = [router._email_trigger_group(key, settings) for key in packages]
        self.assertEqual(len(groups), len(set(groups)))
        self.assertEqual(groups[1], "Nexus email puppy-premium-entry")
        self.assertTrue(all(group.startswith("Nexus email ") for group in groups))

    def test_email_trigger_group_rejects_unsafe_or_non_parameterized_templates(self):
        with self.assertRaises(RuntimeError):
            router._email_trigger_group("puppy:premium-entry", {"email_trigger_group_template": "Чужая {package_key}"})
        with self.assertRaises(RuntimeError):
            router._email_trigger_group("puppy:premium-entry", {"email_trigger_group_template": "Nexus email одна"})

    def test_email_package_storage_is_once_per_user_and_logical_package(self):
        async def scenario() -> None:
            previous_db, previous_ready, previous_error = router._db_path, router._db_ready, router._init_error
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    router._db_path = Path(temp_dir) / "onboarding.db"
                    router._db_ready = asyncio.Event()
                    router._init_error = ""
                    await router._init_db()
                    settings = await router._settings()
                    item = {
                        "gc_user_id": "512982529", "email": "student@example.com", "name": "Ученица",
                        "package_key": "puppy:premium-entry", "source_kind": "upgrade", "source_id": "3",
                        "source_order_id": "880239014", "course_key": "puppy", "tariff": "premium",
                        "stream": "58", "vk_link": "https://vk.example/58", "tg_link": "https://t.me/58",
                        "template_key": "upgrade_premium", "subject": "Первый вариант", "body": "Текст 1",
                    }
                    first = await router._store_email_package(item, settings, force_hold=True)
                    second = await router._store_email_package(
                        {**item, "source_kind": "order", "source_id": "9", "body": "Текст 2"},
                        settings,
                        force_hold=True,
                    )
                    self.assertEqual(first, second)
                    db = await router._connect()
                    try:
                        rows = await (await db.execute("SELECT * FROM email_packages")).fetchall()
                    finally:
                        await db.close()
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["status"], "held")
                    self.assertEqual(rows[0]["body"], "Текст 2")
                    self.assertEqual(rows[0]["operation_id"], router.hashlib.sha256(b"email:512982529:puppy:premium-entry").hexdigest()[:32])
            finally:
                router._db_path, router._db_ready, router._init_error = previous_db, previous_ready, previous_error

        asyncio.run(scenario())

    def test_flow_transition_email_is_once_per_course_and_premium_vip_package(self):
        async def scenario() -> None:
            previous_db, previous_ready, previous_error = router._db_path, router._db_ready, router._init_error
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    router._db_path = Path(temp_dir) / "onboarding.db"
                    router._db_ready = asyncio.Event()
                    router._init_error = ""
                    await router._init_db()
                    first = await router.service_queue_flow_email(
                        gc_user_id="100", email="student@example.com", order_id="200",
                        course_key="dog", course="Собака", stream="54",
                        vk_link="https://vk.example/54", tg_link="https://t.me/54",
                    )
                    second = await router.service_queue_flow_email(
                        gc_user_id="100", email="student@example.com", order_id="200",
                        course_key="dog", course="Собака", stream="55",
                        vk_link="https://vk.example/55", tg_link="https://t.me/55",
                    )
                    self.assertEqual(first["package_id"], second["package_id"])
                    self.assertEqual(first["package_key"], "dog:premium-entry")
                    db = await router._connect()
                    try:
                        row = await (await db.execute("SELECT * FROM email_packages")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["stream"], "55")
                    self.assertEqual(row["template_key"], "flow_transition")
            finally:
                router._db_path, router._db_ready, router._init_error = previous_db, previous_ready, previous_error

        asyncio.run(scenario())

    def test_browser_upgrade_is_disabled_by_default_and_replacement_number_is_stable(self):
        self.assertEqual(router.DEFAULT_SETTINGS["upgrade_browser_enabled"], "0")
        job = {"id": 7, "upgrade_deal_number": "14723", "replacement_deal_number": ""}
        self.assertEqual(router._replacement_deal_number(job), "900000007")
        self.assertEqual(
            router._replacement_deal_number({**job, "replacement_deal_number": "900123456"}),
            "900123456",
        )
        self.assertEqual(
            router._replacement_deal_number({**job, "replacement_deal_number": "NXP-SAVED"}),
            "900000007",
        )

    def test_browser_storage_state_requires_exact_school_cookie(self):
        with self.assertRaises(Exception):
            router._valid_browser_storage_state({"cookies": [], "origins": []})
        state = router._valid_browser_storage_state({
            "cookies": [{"name": "session", "value": "secret", "domain": ".sobakovod.pro"}],
            "origins": [{"origin": "https://club.sobakovod.pro", "localStorage": []}],
        })
        self.assertEqual(state["cookies"][0]["domain"], ".sobakovod.pro")

    def test_browser_navigation_does_not_report_a_slow_getcourse_page_as_lost_session(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        goto = source.index("async def _goto")
        authenticated = source.index("await _authenticated(page, base_url)", goto)
        self.assertIn('wait_until="commit"', source[goto:authenticated])
        self.assertIn('wait_for_load_state("domcontentloaded", timeout=5_000)', source[goto:authenticated])

    def test_payment_save_waits_before_authoritative_reload(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        click = source.index('get_by_role("button", name="Сохранить", exact=True).click()')
        wait = source.index("page.wait_for_timeout(2_000)", click)
        reopen = source.index("await _goto(page, payment_url, base_url)", wait)
        self.assertLess(click, wait)
        self.assertLess(wait, reopen)

    def test_payment_transfer_accepts_only_the_known_fiscal_receipt_warning(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        warning = source.index("уже выбит чек полной оплаты")
        confirmation = source.index("await confirm.click()", warning)
        persisted_check = source.index('if current_deal != target_deal_number', confirmation)
        self.assertLess(warning, confirmation)
        self.assertLess(confirmation, persisted_check)

    def test_payment_lookup_ignores_history_links_outside_the_ledger_table(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count('if await row.count() != 1:'), 2)
        self.assertGreaterEqual(source.count('row_text = _clean(await row.inner_text(), 2000)'), 2)

    def test_completed_order_falls_back_to_the_exact_total_row(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        no_input = source.index("elif price_count == 0:")
        total = source.index('get_by_text("Сумма заказа", exact=True)', no_input)
        mismatch = source.index("if abs(cost - expected_cost) > 0.01", total)
        self.assertLess(no_input, total)
        self.assertLess(total, mismatch)

    def test_paid_order_completion_reuses_the_existing_payment(self):
        source = Path(router.__file__).with_name("gc_browser_action.py").read_text(encoding="utf-8")
        start = source.index("async def _complete_order")
        paid_guard = source.index("expected_received <= 0", start)
        exact_status = source.index("element.value = 'payed'", paid_guard)
        save = source.index("button[name=\"save\"]", exact_status)
        verify = source.index('"expected_status": "Завершен"', save)
        self.assertLess(paid_guard, exact_status)
        self.assertLess(exact_status, save)
        self.assertLess(save, verify)

    def test_replacement_ledger_must_be_fully_paid_by_original_amount(self):
        job = {"source_payed": 13900}
        self.assertEqual(
            router._replacement_ledger_error(job, {"cost_money": 13900, "payed_money": 13900}), "",
        )
        self.assertIn(
            "оплата нового Premium-заказа",
            router._replacement_ledger_error(job, {"cost_money": 13900, "payed_money": 0}),
        )

    def test_replacement_can_be_resolved_by_reserved_number_after_browser_checkpoint(self):
        job = {
            "id": 3,
            "gc_user_id": "512982529",
            "replacement_order_id": "",
            "replacement_deal_number": "900000003",
        }
        exact = {"order_id": "880587895", "deal_number": "900000003", "gc_user_id": "512982529"}
        snapshots = {
            "__related__": [
                {"order_id": "other", "deal_number": "14720", "gc_user_id": "512982529"},
                exact,
            ]
        }
        self.assertEqual(router._replacement_from_snapshots(job, snapshots), exact)

    def test_legacy_repair_resumes_only_from_exact_post_transfer_ledgers(self):
        source = inspect.getsource(router._process_browser_legacy_repair_job)
        branch = source.index('if status == "repair_origin_opening"')
        moved = source.index("payment_already_moved", branch)
        surcharge = source.index("surcharge_changed", moved)
        replacement = source.index("_replacement_ledger_error(job, replacement)", surcharge)
        browser = source.index('"action": "transfer_payment"', replacement)
        self.assertLess(moved, surcharge)
        self.assertLess(surcharge, replacement)
        self.assertLess(replacement, browser)

    def test_repair_checkpoint_requires_exact_private_operation_and_target(self):
        original_db_path = router._db_path
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                router._db_path = Path(temp_dir) / "getcourse-onboarding.db"
                job = {
                    "id": 3,
                    "operation_id": "op-3",
                    "origin_order_id": "880226738",
                    "origin_deal_number": "14721",
                    "replacement_deal_number": "900000003",
                    "source_payed": 13900,
                }
                journal = router._browser_journal_path("repair-op-3")
                journal.write_text(json.dumps({
                    "step": "payment_moved",
                    "operation_id": "repair-op-3",
                    "source_order_id": "880226738",
                    "source_deal_number": "14721",
                    "target_deal_number": "900000003",
                    "target_order_url": "https://club.sobakovod.pro/sales/control/deal/update/id/880587895",
                    "payment_id": "126269153",
                    "expected_amount": 13900,
                }), encoding="utf-8")
                job["browser_journal"] = str(journal)
                self.assertEqual(router._repair_payment_checkpoint(job)["payment_id"], "126269153")
                job["origin_deal_number"] = "14722"
                self.assertEqual(router._repair_payment_checkpoint(job), {})
        finally:
            router._db_path = original_db_path

    def test_legacy_repair_uses_original_standard_and_surcharge_offers(self):
        job = {
            "snapshot_json": json.dumps({
                "origin": {"offer_id": "7846822"},
                "upgrade": {"offer_id": "8623911"},
            })
        }
        self.assertEqual(router._legacy_repair_offers(job), ("7846822", "8623911"))
        self.assertEqual(router._legacy_repair_offers({"snapshot_json": "{}"}), ("", ""))

    def test_legacy_repair_restores_history_only_after_paid_premium_is_verified(self):
        source = inspect.getsource(router._process_browser_legacy_repair_job)
        finalizing = source.index('if status == "repair_replacement_finalizing"')
        ledger = source.index("replacement_error = _replacement_ledger_error", finalizing)
        premium_access = source.index("_upgrade_premium_present", ledger)
        restore_origin = source.index("offer_id=origin_offer", premium_access)
        restore_state = source.index('"repair_restoring_origin"', restore_origin)
        restoring_origin = source.index('if status == "repair_restoring_origin"')
        restore_surcharge = source.index("offer_id=surcharge_offer", restoring_origin)
        self.assertLess(finalizing, ledger)
        self.assertLess(ledger, premium_access)
        self.assertLess(premium_access, restore_origin)
        self.assertLess(restore_origin, restore_state)
        self.assertLess(restoring_origin, restore_surcharge)

    def test_browser_strategy_cancels_standard_through_verified_order_ui(self):
        source = inspect.getsource(router._process_browser_upgrade_job)
        premium = source.index("_upgrade_premium_present")
        cancel = source.index('"action": "cancel_order"', premium)
        zero_payment = source.index('"expected_received": 0', cancel)
        next_state = source.index('"origin_canceling"', zero_payment)
        self.assertLess(premium, cancel)
        self.assertLess(cancel, zero_payment)
        self.assertLess(zero_payment, next_state)

    def test_browser_strategy_completes_paid_replacement_before_final_verification(self):
        source = inspect.getsource(router._process_browser_upgrade_job)
        moved = source.index("_set_upgrade_browser_result")
        complete = source.index('"action": "complete_order"', moved)
        paid = source.index('"expected_received": float(job.get("source_payed")', complete)
        finalizing = source.index('"replacement_finalizing"', paid)
        self.assertLess(moved, complete)
        self.assertLess(complete, paid)
        self.assertLess(paid, finalizing)

    def test_repair_uses_browser_truth_for_stale_offer_exports(self):
        source = inspect.getsource(router._process_browser_legacy_repair_job)
        origin = source.index('if status == "repair_restoring_origin"')
        cancel = source.index('"action": "cancel_order"', origin)
        surcharge = source.index('if status == "repair_restoring_surcharge"', cancel)
        inspect_order = source.index('"action": "inspect_order"', surcharge)
        self.assertLess(origin, cancel)
        self.assertLess(cancel, surcharge)
        self.assertLess(surcharge, inspect_order)

    def test_repair_final_check_uses_exact_orders_and_current_group_panel(self):
        source = inspect.getsource(router._process_browser_legacy_repair_job)
        stage = source.index('if status == "repair_restoring_surcharge"')
        surcharge = source.index('f"repair-surcharge-', stage)
        premium = source.index('f"repair-premium-', surcharge)
        access = source.index('"action": "inspect_access"', premium)
        complete = source.index('"completed"', access)
        self.assertLess(stage, surcharge)
        self.assertLess(surcharge, premium)
        self.assertLess(premium, access)
        self.assertLess(access, complete)

    def test_legacy_repair_acknowledgement_does_not_depend_on_async_export_file(self):
        source = inspect.getsource(router.repair_legacy_upgrade)
        self.assertNotIn("_upgrade_snapshots", source)
        worker = inspect.getsource(router._process_browser_legacy_repair_job)
        self.assertLess(worker.index("_upgrade_snapshots"), worker.index('deal_status="in_work"'))

    def test_legacy_repair_opens_cancelled_source_before_moving_payment(self):
        source = inspect.getsource(router._process_browser_legacy_repair_job)
        ready = source.index('if status == "repair_replacement_ready"')
        open_source = source.index('deal_status="in_work"', ready)
        opening = source.index('if status == "repair_origin_opening"', open_source)
        transfer = source.index('"action": "transfer_payment"', opening)
        restore = source.index("offer_id=origin_offer", transfer)
        cancel = source.index('settings["upgrade_command_field"]: "prepare"', restore)
        self.assertLess(ready, open_source)
        self.assertLess(open_source, opening)
        self.assertLess(opening, transfer)
        self.assertLess(transfer, restore)
        self.assertLess(restore, cancel)

    def test_browser_strategy_creates_new_order_before_any_standard_cancellation(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                originals = (
                    router._upgrade_snapshots, router._run_browser_action,
                    router._mutate_upgrade_order, router._upgrade_event,
                )
                router._db_path = Path(directory) / "module.db"
                calls = []

                async def snapshots(_job, *, live):
                    self.assertTrue(live)
                    return {
                        "origin": {"status": "Завершен", "cost_money": 13900, "payed_money": 13900},
                        "surcharge": {"status": "Завершен", "payment_state": "paid", "cost_money": 5000, "payed_money": 5000},
                        "__related__": [],
                    }

                async def browser(payload, **_kwargs):
                    self.assertEqual(payload["action"], "probe")
                    return {"ok": True}

                async def mutate(_job, **changes):
                    calls.append(changes)
                    return {"ok": True}

                async def event(*_args, **_kwargs):
                    return None

                try:
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            """INSERT INTO upgrade_jobs(
                               upgrade_order_id,upgrade_deal_number,origin_order_id,origin_deal_number,gc_user_id,
                               course_key,source_offer_id,target_offer_id,source_cost,source_payed,upgrade_cost,
                               upgrade_payed,status,approved,operation_id,strategy,created_at,updated_at
                               ) VALUES('surcharge','14723','origin','14721','512982529','puppy','7846822',
                               '7846827',13900,13900,5000,5000,'validated',1,'op-1','replacement_browser',?,?)""",
                            (now, now),
                        )
                        await db.commit()
                        job = dict(await (await db.execute("SELECT * FROM upgrade_jobs WHERE id=1")).fetchone())
                    finally:
                        await db.close()
                    router._upgrade_snapshots = snapshots
                    router._run_browser_action = browser
                    router._mutate_upgrade_order = mutate
                    router._upgrade_event = event
                    settings = {
                        **router.DEFAULT_SETTINGS,
                        "upgrade_browser_enabled": "1",
                        "upgrade_link_field": "link",
                        "upgrade_operation_field": "operation",
                    }
                    await router._process_browser_upgrade_job(job, settings)
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(calls[0]["deal_number"], "900000001")
                    self.assertEqual(calls[0]["deal_cost"], 13900)
                    self.assertEqual(calls[0]["offer_id"], "7846827")
                    self.assertNotIn(settings["upgrade_command_field"], calls[0]["addfields"])
                    db = await router._connect()
                    try:
                        saved = await (await db.execute(
                            "SELECT status,replacement_deal_number FROM upgrade_jobs WHERE id=1"
                        )).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual((saved["status"], saved["replacement_deal_number"]), ("replacement_creating", "900000001"))
                finally:
                    (
                        router._upgrade_snapshots, router._run_browser_action,
                        router._mutate_upgrade_order, router._upgrade_event,
                    ) = originals
                    router._db_path = previous_db

        asyncio.run(run())

    def test_upgrade_offer_matrix_and_test_mode_are_safe_defaults(self):
        settings = dict(router.DEFAULT_SETTINGS)
        self.assertEqual(settings["upgrade_mode"], "test")
        self.assertEqual(settings["upgrade_enabled"], "0")
        self.assertEqual(router._upgrade_target_offer("dog", False, settings), "7673858")
        self.assertEqual(router._upgrade_target_offer("dog", True, settings), "8043443")
        self.assertEqual(router._upgrade_target_offer("puppy", False, settings), "7846896")
        self.assertEqual(router._upgrade_target_offer("puppy", True, settings), "7846827")
        self.assertEqual(router._upgrade_target_offer("combo", False, settings), "7846898")
        self.assertEqual(router._upgrade_target_offer("combo", True, settings), "7846828")

    def test_getcourse_finalize_markers_are_exact_and_operation_id_stays_separate(self):
        source = Path(router.__file__).read_text(encoding="utf-8")
        self.assertIn('settings["upgrade_command_field"]: "prepare"', source)
        self.assertIn('settings["upgrade_command_field"]: "finalize"', source)
        self.assertIn('settings["upgrade_command_field"]: "finalize_rollback"', source)
        self.assertNotIn('f"prepare:{job.get', source)
        self.assertNotIn('f"finalize:{job.get', source)
        self.assertNotIn('f"finalize_rollback:{job.get', source)

    def test_upgrade_financial_and_access_invariants(self):
        job = {"source_cost": 15900, "source_payed": 15900, "upgrade_cost": 11000, "upgrade_payed": 11000}
        origin = {"cost_money": 15900, "payed_money": 15900}
        surcharge = {"cost_money": 11000, "payed_money": 11000}
        self.assertEqual(router._upgrade_ledger_error(job, origin, surcharge), "")
        self.assertIn(
            "стоимость исходного заказа",
            router._upgrade_ledger_error(job, {**origin, "cost_money": 26900}, surcharge),
        )
        combo_premium = [
            {"name": "Щенок Премиум"}, {"name": "Собака Премиум"},
        ]
        self.assertTrue(router._upgrade_access_ready("combo", combo_premium))
        self.assertFalse(router._upgrade_access_ready("combo", combo_premium + [{"name": "Собака Стандарт"}]))
        self.assertTrue(
            router._upgrade_access_ready(
                "combo", [{"name": "Щенок Стандарт"}, {"name": "Собака Стандарт"}], rollback=True,
            )
        )

    def test_upgrade_order_accepts_getcourse_stale_offer_only_with_matching_premium_course(self):
        job = {"target_offer_id": "7846827", "course_key": "puppy"}
        self.assertTrue(router._upgrade_premium_order_matches(job, {
            "offer_id": "7846827", "tariff": "", "course_key": "",
        }))
        self.assertTrue(router._upgrade_premium_order_matches(job, {
            "offer_id": "8623911", "tariff": "premium", "course_key": "puppy",
        }))
        self.assertFalse(router._upgrade_premium_order_matches(job, {
            "offer_id": "8623911", "tariff": "standard", "course_key": "puppy",
        }))
        self.assertFalse(router._upgrade_premium_order_matches(job, {
            "offer_id": "8623911", "tariff": "premium", "course_key": "dog",
        }))

    def test_upgrade_candidate_is_persisted_as_preview_without_external_write(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    candidate = {
                        "source_record_id": 20, "order_id": "upgrade-20", "deal_number": "U-20",
                        "gc_user_id": "512982529", "name": "Ученик", "email": "student@example.test",
                        "course_key": "combo", "offer_id": "900001", "cost_money": 12000,
                        "payed_money": 12000, "paid_at": "2026-08-16T10:00:00Z",
                        "origins": [{
                            "source_record_id": 10, "order_id": "standard-10", "deal_number": "S-10",
                            "gc_user_id": "512982529", "course_key": "combo", "offer_id": "700001",
                            "cost_money": 30900, "payed_money": 30900,
                            "paid_at": "2026-08-10T10:00:00Z", "autopayment": False,
                        }],
                    }
                    job_id = await router._upsert_upgrade_candidate(candidate, dict(router.DEFAULT_SETTINGS))
                    job = await router._upgrade_job(job_id)
                    self.assertEqual(job["status"], "preview")
                    self.assertEqual(job["approved"], 0)
                    self.assertEqual(job["target_offer_id"], "7846898")
                    self.assertEqual(job["source_cost"], 30900)
                    self.assertEqual(job["upgrade_cost"], 12000)
                finally:
                    router._db_path = previous_db

        asyncio.run(run())

    def test_upgrade_worker_never_mutates_getcourse_in_test_mode(self):
        async def run():
            original_settings = router._settings

            async def settings():
                return {**router.DEFAULT_SETTINGS, "upgrade_enabled": "1", "upgrade_mode": "test"}

            router._settings = settings
            try:
                self.assertEqual(await router._process_due_upgrades(), 0)
            finally:
                router._settings = original_settings

        asyncio.run(run())

    def test_upgrade_chat_uses_links_saved_on_original_order(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    await router._upsert_order(
                        {
                            "source_record_id": 10, "order_id": "standard-10", "deal_number": "S-10",
                            "gc_user_id": "512982529", "name": "Ученик", "email": "student@example.test",
                            "paid_at": "2026-08-16T10:00:00Z", "course_key": "puppy", "course": "Щенок",
                            "tariff": "standard", "flow": {
                                "stream": "58", "vk_link": "https://vk.example/58", "tg_link": "https://t.me/58",
                            },
                        },
                        12,
                    )
                    router._module = lambda *_: (_ for _ in ()).throw(
                        AssertionError("saved original flow must avoid external catalog lookup")
                    )
                    flows = await router._upgrade_flows(
                        {"origin_order_id": "standard-10", "origin_paid_at": "2026-08-16T10:00:00Z", "course_key": "puppy"}
                    )
                    self.assertEqual(flows["puppy"]["stream"], "58")
                    self.assertEqual(flows["puppy"]["vk_link"], "https://vk.example/58")
                    self.assertEqual(flows["puppy"]["tg_link"], "https://t.me/58")
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_test_mode_is_default_and_blocks_due_processing(self):
        self.assertEqual(router.DEFAULT_SETTINGS["delivery_mode"], "test")

        async def run():
            original_settings, original_due = router._settings, router._due_rows

            async def settings():
                return {**router.DEFAULT_SETTINGS, "delivery_mode": "test"}

            async def due_rows():
                raise AssertionError("test mode must not inspect or deliver due rows")

            router._settings, router._due_rows = settings, due_rows
            try:
                self.assertEqual(await router._process_due(), 0)
            finally:
                router._settings, router._due_rows = original_settings, original_due

        asyncio.run(run())

    def test_classifies_sale_source_from_getcourse_autopayment_marker(self):
        self.assertEqual(router._scenario_branch({"tariff": "premium", "autopayment": "0"}), "manager_premium")
        self.assertEqual(router._scenario_branch({"tariff": "vip", "autopayment": ""}), "manager_premium")
        self.assertEqual(router._scenario_branch({"tariff": "standard", "autopayment": False}), "manager_standard")
        self.assertEqual(router._scenario_branch({"tariff": "premium", "autopayment": "1"}), "autopay_premium")
        self.assertEqual(router._scenario_branch({"tariff": "vip", "autopayment": True}), "autopay_premium")
        self.assertEqual(router._scenario_branch({"tariff": "standard", "autopayment": "автооплата"}), "autopay_standard")

    def test_welcome_text_depends_on_tariff_while_scenario_stays_explicit(self):
        self.assertEqual(router._template_key_for_order({"tariff": "premium", "branch": "manager_premium"}), "manager")
        self.assertEqual(router._template_key_for_order({"tariff": "vip", "branch": "manager_premium"}), "manager")
        self.assertEqual(router._template_key_for_order({"tariff": "vip", "branch": "autopay_premium"}), "autopay_vip")
        self.assertEqual(router._template_key_for_order({"tariff": "premium", "branch": "autopay_premium"}), "premium")
        self.assertEqual(router._template_key_for_order({"tariff": "standard", "branch": "manager_standard"}), "manager_standard")
        self.assertEqual(router._template_key_for_order({"tariff": "standard", "branch": "autopay_standard"}), "standard")

    def test_standard_copy_never_mentions_a_text_instruction(self):
        legacy = (
            "Смотреть видеоинструкцию: {video_instruction_url}\n"
            "Смотреть текстовую инструкцию: {text_instruction_url}\n\nВАЖНО"
        )
        rendered = router._template_body("standard", legacy)
        self.assertIn("Смотреть видеоинструкцию", rendered)
        self.assertIn("ВАЖНО", rendered)
        self.assertNotIn("текстовую инструкцию", rendered.casefold())
        self.assertNotIn("text_instruction_url", rendered)
        self.assertNotIn("текстовую инструкцию", router.WELCOME_STANDARD.casefold())

    def test_pending_orders_use_getcourse_source_not_amo_responsible(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                router._db_path = Path(directory) / "module.db"

                try:
                    await router._init_db()
                    common = {
                        "source_record_id": 1, "deal_number": "1", "gc_user_id": "1",
                        "name": "Ученик", "email": "student@example.test",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "autopayment": "", "manager_name": "", "utm_term": "platform_id=1", "flow": {},
                    }
                    await router._upsert_order({**common, "order_id": "manager", "phone": "+79990000001", "tariff": "premium"}, 12)
                    await router._upsert_order({**common, "source_record_id": 2, "order_id": "auto", "phone": "+79990000002", "tariff": "standard", "autopayment": "1", "autopayment_source": "title"}, 12)
                    await router._upsert_order({**common, "source_record_id": 3, "order_id": "manager-standard", "phone": "+79990000001", "tariff": "standard"}, 12)
                    await router._upsert_order({**common, "source_record_id": 4, "order_id": "auto-vip", "phone": "+79990000002", "tariff": "vip", "autopayment": True}, 12)
                    self.assertEqual(await router._classify_pending_orders(), 4)
                    db = await router._connect()
                    try:
                        rows = {
                            row["order_id"]: (row["branch"], row["status"])
                            for row in await (await db.execute("SELECT order_id,branch,status FROM orders")).fetchall()
                        }
                    finally:
                        await db.close()
                    self.assertEqual(rows["manager"], ("manager_premium", "pending"))
                    self.assertEqual(rows["auto"], ("autopay_standard", "pending"))
                    self.assertEqual(rows["manager-standard"], ("manager_standard", "pending"))
                    self.assertEqual(rows["auto-vip"], ("autopay_premium", "pending"))
                finally:
                    router._db_path = previous_db

        asyncio.run(run())

    def test_only_autopayment_orders_receive_access_reminder(self):
        self.assertFalse(router._reminder_enabled({"branch": "manager_premium"}))
        self.assertFalse(router._reminder_enabled({"branch": "manager_standard"}))
        self.assertTrue(router._reminder_enabled({"branch": "autopay_premium"}))
        self.assertTrue(router._reminder_enabled({"branch": "autopay_standard"}))

    def test_manager_templates_have_no_upgrade_offer(self):
        for body in (router.WELCOME_MANAGER, router.WELCOME_MANAGER_STANDARD):
            self.assertNotIn("{upgrade_url}", body)
            self.assertNotIn("доплат", body.casefold())
        self.assertIn("{upgrade_url}", router.WELCOME_STANDARD)
        self.assertIn("{upgrade_url}", router.WELCOME_PREMIUM)
        self.assertNotIn("{upgrade_url}", router.WELCOME_AUTOPAY_VIP)

    def test_welcome_schedules_reminder_only_for_autopayment(self):
        async def run(branch):
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                router._db_path = Path(directory) / "module.db"
                sent = []
                originals = (
                    router._ensure_order_flow,
                    router._template,
                    router._send_stage_with_fallback,
                )

                async def flow(row):
                    return {**row, "stream": "60", "vk_link": "https://vk.test", "tg_link": "https://tg.test"}

                async def template(_key):
                    return "Привет"

                async def send(_row, _body, stage, _settings, **_kwargs):
                    sent.append(stage)
                    return ["1"], {"provider": "vk", "recipient_id": "123"}

                try:
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            """INSERT INTO orders(source_record_id,order_id,paid_at,course_key,course,tariff,
                               autopayment,branch,status,welcome_due_at,reminder_due_at,created_at,updated_at)
                               VALUES(1,?,?,?,?,?,?,?,'pending',?,?,?,?)""",
                            (
                                branch, "2026-08-12T00:00:00Z", "dog", "Собака", "vip",
                                "1" if branch.startswith("autopay_") else "0", branch,
                                now, now, now, now,
                            ),
                        )
                        await db.commit()
                        row = dict(await (await db.execute("SELECT * FROM orders")).fetchone())
                    finally:
                        await db.close()
                    (
                        router._ensure_order_flow,
                        router._template,
                        router._send_stage_with_fallback,
                    ) = (flow, template, send)
                    await router._send_welcome(row, dict(router.DEFAULT_SETTINGS))
                    db = await router._connect()
                    try:
                        stored = await (await db.execute("SELECT reminder_due_at FROM orders")).fetchone()
                    finally:
                        await db.close()
                    return stored[0], sent
                finally:
                    (
                        router._ensure_order_flow,
                        router._template,
                        router._send_stage_with_fallback,
                    ) = originals
                    router._db_path = previous_db

        manager_due, manager_sent = asyncio.run(run("manager_premium"))
        autopay_due, autopay_sent = asyncio.run(run("autopay_premium"))
        self.assertEqual(manager_due, "")
        self.assertTrue(autopay_due)
        self.assertEqual(manager_sent, ["welcome"])
        self.assertEqual(autopay_sent, ["welcome"])

    def test_tariff_and_course_upgrade_links_are_kept_separate(self):
        settings = dict(router.DEFAULT_SETTINGS)
        puppy = router._render(
            "{upgrade_url}",
            {"tariff": "standard", "course_key": "puppy", "course": "Щенок"},
            settings,
        )
        dog = router._render(
            "{upgrade_url}",
            {"tariff": "standard", "course_key": "dog", "course": "Собака"},
            settings,
        )
        premium = router._render("{upgrade_url}", {"tariff": "premium"}, settings)
        self.assertEqual(
            puppy,
            "https://club.sobakovod.pro/doplata_premium_puppy?utm_medium=perevodpismo",
        )
        self.assertEqual(
            dog,
            "https://club.sobakovod.pro/doplata_premium_dog?utm_medium=perevodpismo",
        )
        self.assertEqual(
            premium,
            "https://club.sobakovod.pro/doplata_vip?utm_medium=perevodpismo",
        )
        self.assertIn("проверка домашних заданий", router.WELCOME_PREMIUM)
        self.assertIn("тариф до уровня VIP", router.WELCOME_PREMIUM)

    def test_course_upgrade_link_keeps_legacy_fallback(self):
        settings = {
            **router.DEFAULT_SETTINGS,
            "standard_upgrade_puppy_url": "",
            "standard_upgrade_dog_url": "",
            "standard_upgrade_url": "https://example.com/legacy",
        }
        self.assertEqual(
            router._render(
                "{upgrade_url}",
                {"tariff": "standard", "course": "Собака"},
                settings,
            ),
            "https://example.com/legacy",
        )

    def test_panel_exposes_both_standard_course_links(self):
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="standardUpgradePuppyUrl"', panel)
        self.assertIn('id="standardUpgradeDogUrl"', panel)
        self.assertIn("standard_upgrade_puppy_url", panel)
        self.assertIn("standard_upgrade_dog_url", panel)
        self.assertNotIn('id="standardUpgradeUrl"', panel)

    def test_panel_names_all_four_business_scenarios(self):
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        for branch in ("manager_premium", "manager_standard", "autopay_premium", "autopay_standard"):
            self.assertIn(branch, panel)
        self.assertNotIn("Тариф Premium'", panel)

    def test_panel_offers_manual_identity_only_for_missing_recipient(self):
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn("missingIdentity=o.status==='waiting_identity'", panel)
        self.assertIn('id="manualPlatformId"', panel)
        self.assertIn('id="manualSalebotId"', panel)
        self.assertIn("data-retry-identity", panel)
        self.assertIn("Проверяем и отправляем…", panel)
        self.assertIn("JSON.stringify(identity)", panel)

    def test_retry_missing_recipient_accepts_platform_and_salebot_id(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, previous_auth, previous_module = router._db_path, router._require_admin, router._module
                router._db_path = Path(directory) / "module.db"
                seen_terms = []

                async def allow(_request):
                    return {"role": "admin"}

                class Incoming:
                    headers = {"content-type": "application/json"}

                    def __init__(self, payload):
                        self.payload = payload

                    async def json(self):
                        return self.payload

                class Messenger:
                    @staticmethod
                    async def service_resolve_onboarding_telegram_target(**kwargs):
                        seen_terms.append(kwargs["utm_term"])
                        return {"ok": True, "platform_id": "5601500902"}

                try:
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        for source_id, order_id in ((1, "manual-platform"), (2, "manual-salebot")):
                            await db.execute(
                                """INSERT INTO orders(source_record_id,order_id,paid_at,course_key,course,tariff,
                                   status,error,welcome_due_at,reminder_due_at,created_at,updated_at)
                                   VALUES(?,?,?,'dog','Собака','premium','waiting_identity','Получатель не найден',?,'',?,?)""",
                                (source_id, order_id, now, now, now, now),
                            )
                        await db.commit()
                    finally:
                        await db.close()
                    router._require_admin = allow
                    router._module = lambda _module_id, _service: Messenger()

                    direct = await router.retry_order(1, Incoming({"platform_id": "5601500901", "salebot_id": ""}))
                    linked = await router.retry_order(2, Incoming({"platform_id": "", "salebot_id": "88001"}))
                    self.assertEqual(direct["identity_source"], "platform_id")
                    self.assertEqual(linked["identity_source"], "salebot_id")
                    self.assertEqual(seen_terms, ["salebot_id=88001"])

                    db = await router._connect()
                    try:
                        rows = await (await db.execute(
                            "SELECT order_id,status,target_source,target_platform_id,error FROM orders ORDER BY id"
                        )).fetchall()
                    finally:
                        await db.close()
                    self.assertEqual(
                        [tuple(row) for row in rows],
                        [
                            ("manual-platform", "pending", "vk", "5601500901", ""),
                            ("manual-salebot", "pending", "telegram", "5601500902", ""),
                        ],
                    )
                    await db.close()
                    db = await router._connect()
                    columns = await (await db.execute(
                        "SELECT manual_vk_platform_id,manual_telegram_platform_id FROM orders ORDER BY id"
                    )).fetchall()
                    self.assertEqual([tuple(row) for row in columns], [("5601500901", ""), ("", "5601500902")])
                finally:
                    router._module = previous_module
                    router._require_admin = previous_auth
                    router._db_path = previous_db

        asyncio.run(run())

    def test_retry_missing_recipient_accepts_both_manual_identities(self):
        async def run():
            previous_auth = router._require_admin

            async def allow(_request):
                return {"role": "admin"}

            class Incoming:
                headers = {"content-type": "application/json"}

                async def json(self):
                    return {"platform_id": "5601500901", "salebot_id": "88001"}

            try:
                router._require_admin = allow
                with tempfile.TemporaryDirectory() as directory:
                    previous_db, previous_module = router._db_path, router._module
                    router._db_path = Path(directory) / "module.db"
                    class Messenger:
                        @staticmethod
                        async def service_resolve_onboarding_telegram_target(**kwargs):
                            return {"ok": True, "platform_id": "5601500902"}
                    try:
                        await router._init_db()
                        db = await router._connect()
                        try:
                            now = router._iso()
                            await db.execute(
                                """INSERT INTO orders(source_record_id,order_id,paid_at,course_key,course,tariff,
                                   status,error,welcome_due_at,reminder_due_at,created_at,updated_at)
                                   VALUES(1,'both',?,'dog','Собака','premium','waiting_identity','',?,'',?,?)""",
                                (now, now, now, now),
                            )
                            await db.commit()
                        finally:
                            await db.close()
                        router._module = lambda _module_id, _service: Messenger()
                        result = await router.retry_order(1, Incoming())
                        self.assertEqual(result["identity_source"], "platform_id+salebot_id")
                    finally:
                        router._module = previous_module
                        router._db_path = previous_db
            finally:
                router._require_admin = previous_auth

        asyncio.run(run())

    def test_panel_history_includes_test_runs_and_template_wheel_scroll(self):
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn('data-view="responses">История', panel)
        self.assertIn('class="history-details"', panel)
        self.assertIn("$('templateNav').addEventListener('wheel'", panel)

    def test_response_history_returns_real_and_existing_test_events(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, previous_auth = router._db_path, router._require_admin
                router._db_path = Path(directory) / "module.db"

                async def allow(_request):
                    return {"username": "attackpng"}

                try:
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            """INSERT INTO orders(source_record_id,order_id,name,paid_at,course_key,course,tariff,
                               status,response,responded_at,target_source,target_platform_id,amo_task_id,
                               welcome_due_at,reminder_due_at,created_at,updated_at)
                               VALUES(1,'real-1','Ученик',?,'dog','Собака','premium','help_requested','no',?,
                               'vk','123','555',?,'',?,?)""",
                            (now, now, now, now, now),
                        )
                        await db.execute(
                            """INSERT INTO test_runs(request_id,recipient_ref,recipient_id,requested_by,status,
                               results_json,created_at,updated_at) VALUES('test-1','telegram','456','attackpng',
                               'responded_yes',?, ?, ?)""",
                            (json.dumps([{"mode": "telegram_live_task", "provider": "telegram"}, {"stage": "button_yes"}]), now, now),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    router._require_admin = allow
                    result = await router.responses(object(), limit=20)
                    self.assertEqual({item["kind"] for item in result["items"]}, {"order", "test_live"})
                    test = next(item for item in result["items"] if item["kind"] == "test_live")
                    self.assertEqual((test["choice"], test["channel"], test["request_id"]), ("yes", "telegram", "test-1"))
                    real = next(item for item in result["items"] if item["kind"] == "order")
                    self.assertEqual((real["choice"], real["amo_task_id"]), ("no", "555"))
                finally:
                    router._require_admin = previous_auth
                    router._db_path = previous_db

        asyncio.run(run())

    def test_salebot_not_found_request_is_written_to_history_without_secret(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, previous_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"

                class Messenger:
                    @staticmethod
                    async def service_resolve_onboarding_telegram_target(**_kwargs):
                        return {"ok": False}

                class Incoming:
                    method = "POST"
                    query_params = {}
                    headers = {"content-type": "application/x-www-form-urlencoded"}

                    def __init__(self, secret):
                        self.secret = secret

                    async def body(self):
                        return f"secret={self.secret}&client_id=99999".encode()

                try:
                    await router._init_db()
                    router._module = lambda *_args: Messenger()
                    db = await router._connect()
                    try:
                        secret = (await (await db.execute(
                            "SELECT value FROM settings WHERE key='salebot_help_secret'"
                        )).fetchone())[0]
                    finally:
                        await db.close()
                    response = await router.salebot_help(Incoming(secret))
                    self.assertEqual(response.status_code, 404)
                    db = await router._connect()
                    try:
                        event = await (await db.execute("SELECT * FROM interaction_events")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual((event["source"], event["status"], event["choice"]), ("salebot", "order_not_found", "no"))
                    self.assertEqual(json.loads(event["payload_json"]), {"client_id": "99999"})
                    self.assertNotIn(secret, event["payload_json"])
                finally:
                    router._module = previous_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_splits_long_telegram_message_without_losing_text(self):
        paragraphs = ["A" * 2500, "B" * 2500, "C" * 800]
        chunks = router._split_message("\n\n".join(paragraphs))
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(0 < len(item) <= router.MAX_MESSAGE for item in chunks))
        self.assertEqual("".join(chunks).replace("\n", ""), "".join(paragraphs))

    def test_webhook_guard_stores_only_fingerprint_and_host(self):
        view = router._webhook_view({})
        self.assertEqual(view["fingerprint"], "")
        view = router._webhook_view({"url": "https://senler.example/telegram/super-secret"})
        self.assertEqual(view["host"], "senler.example")
        self.assertNotIn("super-secret", json.dumps(view))

    def test_bot_api_surface_never_manages_or_consumes_updates(self):
        source = Path(router.__file__).read_text(encoding="utf-8")
        forbidden = ["setWebhook", "deleteWebhook", "getUpdates"]
        self.assertFalse([method for method in forbidden if f'"{method}"' in source])
        self.assertIn('_tg_call("sendMessage"', source)
        self.assertIn('_tg_call("getWebhookInfo"', source)

    def test_upsert_keeps_paid_time_and_current_flow(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous = router._db_path
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    first = {
                        "source_record_id": 1, "order_id": "42", "deal_number": "42", "gc_user_id": "7",
                        "name": "Ирина", "email": "i@example.test", "phone": "+79990000000",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "vip", "autopayment": "1", "manager_name": "", "utm_term": "platform_id=123",
                        "flow": {"stream": "60", "vk_link": "https://vk.test/60", "tg_link": "https://t.me/60"},
                    }
                    await router._upsert_order(first, 12)
                    second = dict(first)
                    second["paid_at"] = "2026-08-12T10:00:00Z"
                    second["flow"] = {"stream": "61", "vk_link": "https://vk.test/61", "tg_link": "https://t.me/61"}
                    await router._upsert_order(second, 12)
                    db = await router._connect()
                    try:
                        row = await (await db.execute("SELECT paid_at,stream,branch FROM orders")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["paid_at"], "2026-08-11T10:00:00Z")
                    self.assertEqual(row["stream"], "61")
                    self.assertEqual(row["branch"], "")
                finally:
                    router._db_path = previous

        asyncio.run(run())

    def test_first_seen_full_order_with_prior_partial_is_archived_without_welcome(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, previous_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"

                class Orders:
                    @staticmethod
                    async def service_order_payment_history(**kwargs):
                        self.assertEqual(kwargs["order_id"], "same-order")
                        return {"ok": True, "partial_seen": True, "items": [{"payment_state": "partial"}]}

                try:
                    await router._init_db()
                    router._module = lambda *_: Orders()
                    item = {
                        "source_record_id": 1, "order_id": "same-order", "paid_at": "2026-08-12T10:00:00Z",
                        "course_key": "dog", "course": "Собака", "tariff": "premium", "payment_state": "paid", "flow": {},
                    }
                    self.assertEqual(await router._initial_status_for_source_item(item), "backfill_only")
                    await router._upsert_order(item, 12, initial_status="backfill_only")
                    self.assertEqual(await router._due_rows(), [])
                    # Once the module owns the partial-payment row, a later paid
                    # snapshot is idempotent without consulting the ledger.
                    self.assertEqual(await router._initial_status_for_source_item(item), "")
                finally:
                    router._module = previous_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_archive_rows_are_never_due(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous = router._db_path
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    item = {
                        "source_record_id": 9, "order_id": "archive-9", "name": "Архив",
                        "paid_at": "2026-08-10T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "premium", "autopayment": "1", "flow": {},
                    }
                    await router._upsert_order(item, 12, initial_status="backfill_only")
                    self.assertEqual(await router._due_rows(), [])
                    await router._upsert_order(item, 12)
                    db = await router._connect()
                    try:
                        row = await (await db.execute("SELECT status FROM orders")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["status"], "backfill_only")
                finally:
                    router._db_path = previous

        asyncio.run(run())

    def test_missing_flow_is_recovered_from_persistent_catalog(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"

                class Fields:
                    @staticmethod
                    async def service_resolve_onboarding_flow(**kwargs):
                        self.assertEqual(kwargs["course_key"], "dog")
                        return {
                            "ok": True, "status": "resolved", "stale": True,
                            "source": "flow_students_cache", "errors": [{"error": "429"}],
                            "flow": {
                                "stream": "55", "vk_link": "https://vk.example/55",
                                "tg_link": "https://t.me/example55",
                            },
                        }

                try:
                    await router._init_db()
                    await router._upsert_order({
                        "source_record_id": 22, "order_id": "flow-22", "name": "Ирина",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "vip", "flow": {},
                    }, 12)
                    db = await router._connect()
                    try:
                        await db.execute("UPDATE orders SET branch='autopay',status='waiting_flow' WHERE order_id='flow-22'")
                        await db.commit()
                        row = dict(await (await db.execute("SELECT * FROM orders WHERE order_id='flow-22'")).fetchone())
                    finally:
                        await db.close()
                    router._module = lambda *_: Fields()
                    refreshed = await router._ensure_order_flow(row)
                    self.assertEqual((refreshed["stream"], refreshed["vk_link"]), ("55", "https://vk.example/55"))
                    db = await router._connect()
                    try:
                        stored = await (await db.execute("SELECT stream,status,error FROM orders WHERE order_id='flow-22'")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual((stored["stream"], stored["status"], stored["error"]), ("55", "pending", ""))
                    self.assertEqual(router._last_flow_result["status"], "stale_fallback")
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_incomplete_delivery_is_quarantined_instead_of_repeated(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    await router._upsert_order({
                        "source_record_id": 23, "order_id": "uncertain-23", "name": "Ирина",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "vip", "flow": {
                            "stream": "55", "vk_link": "https://vk.example/55", "tg_link": "https://t.me/example55",
                        },
                    }, 12)
                    first, operation_id = await router._delivery_start(1, "welcome")
                    second, repeated_id = await router._delivery_start(1, "welcome")
                    self.assertTrue(first)
                    self.assertFalse(second)
                    self.assertEqual(operation_id, repeated_id)
                    db = await router._connect()
                    try:
                        order = await (await db.execute("SELECT status,error FROM orders WHERE id=1")).fetchone()
                        delivery = await (await db.execute("SELECT status FROM deliveries WHERE order_row_id=1")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(order["status"], "delivery_uncertain")
                    self.assertIn("автоматический повтор остановлен", order["error"])
                    self.assertEqual(delivery["status"], "uncertain")
                    self.assertEqual(await router._due_rows(), [])
                finally:
                    router._db_path = previous_db

        asyncio.run(run())

    def test_ambiguous_telegram_failure_is_not_retried(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                originals = (
                    router._resolve_targets,
                    router._webhook_guard,
                    router._tg_call,
                    router._send_order_text,
                )
                router._db_path = Path(directory) / "module.db"

                async def ambiguous(*_args, **_kwargs):
                    raise RuntimeError("timeout after request write")

                async def targets(_row):
                    return {"ok": True, "candidates": [{"provider": "telegram", "recipient_id": "123"}]}

                async def guard():
                    return {"ok": True}

                async def telegram(_method, _payload):
                    return {"id": 123, "type": "private"}

                try:
                    await router._init_db()
                    await router._upsert_order({
                        "source_record_id": 24, "order_id": "telegram-24", "name": "Ирина",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "vip", "flow": {
                            "stream": "55", "vk_link": "https://vk.example/55", "tg_link": "https://t.me/example55",
                        },
                    }, 12)
                    db = await router._connect()
                    try:
                        await db.execute(
                            "UPDATE orders SET branch='autopay',status='welcomed',target_platform_id='123',target_source='telegram',welcome_sent_at=? WHERE id=1",
                            (router._iso(),),
                        )
                        await db.commit()
                        row = dict(await (await db.execute("SELECT * FROM orders WHERE id=1")).fetchone())
                    finally:
                        await db.close()
                    router._resolve_targets = targets
                    router._webhook_guard = guard
                    router._tg_call = telegram
                    router._send_order_text = ambiguous
                    with self.assertRaises(router.DeliveryUncertain):
                        await router._send_reminder(row, dict(router.DEFAULT_SETTINGS))
                    db = await router._connect()
                    try:
                        order = await (await db.execute("SELECT status FROM orders WHERE id=1")).fetchone()
                        delivery = await (await db.execute("SELECT status FROM deliveries WHERE order_row_id=1 AND stage='reminder'")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(order["status"], "delivery_uncertain")
                    self.assertEqual(delivery["status"], "uncertain")
                    self.assertEqual(await router._due_rows(), [])
                finally:
                    (
                        router._resolve_targets,
                        router._webhook_guard,
                        router._tg_call,
                        router._send_order_text,
                    ) = originals
                    router._db_path = previous_db

        asyncio.run(run())

    def test_inert_manager_rows_cannot_starve_actionable_queue(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                router._db_path = Path(directory) / "module.db"
                try:
                    await router._init_db()
                    now = router._iso()
                    db = await router._connect()
                    try:
                        for index in range(250):
                            await db.execute(
                                """INSERT INTO orders(source_record_id,order_id,paid_at,course_key,course,tariff,
                                   branch,status,welcome_due_at,welcome_sent_at,reminder_due_at,created_at,updated_at)
                                   VALUES(?,?,?,?,?,?,'manager_premium','welcomed',?,?, '',?,?)""",
                                (index + 1, f"inert-{index}", now, "dog", "Собака", "vip", now, now, now, now),
                            )
                        await db.execute(
                            """INSERT INTO orders(source_record_id,order_id,paid_at,course_key,course,tariff,
                               branch,status,welcome_due_at,reminder_due_at,created_at,updated_at)
                               VALUES(999,'actionable',?,'dog','Собака','vip','manager_premium','pending',?,'',?,?)""",
                            (now, now, now, now),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    due = await router._due_rows()
                    self.assertEqual([row["order_id"] for row in due], ["actionable"])
                finally:
                    router._db_path = previous_db

        asyncio.run(run())

    def test_delivery_processing_is_bounded_and_concurrent(self):
        async def run():
            originals = router._settings, router._due_rows, router._send_welcome
            active = maximum = 0

            async def settings():
                return {**router.DEFAULT_SETTINGS, "delivery_mode": "live"}

            async def due_rows():
                return [
                    {"id": index, "order_id": str(index), "status": "pending", "welcome_sent_at": "", "welcome_due_at": router._iso()}
                    for index in range(20)
                ]

            async def send(_row, _settings):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                await asyncio.sleep(0.01)
                active -= 1

            try:
                router._settings, router._due_rows, router._send_welcome = settings, due_rows, send
                self.assertEqual(await router._process_due(), 20)
                self.assertGreater(maximum, 1)
                self.assertLessEqual(maximum, router.DELIVERY_CONCURRENCY)
            finally:
                router._settings, router._due_rows, router._send_welcome = originals

        asyncio.run(run())

    def test_definitive_telegram_rejection_falls_back_to_vk(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db = router._db_path
                originals = router._resolve_targets, router._webhook_guard, router._tg_call, router._send_order_text
                router._db_path = Path(directory) / "module.db"
                sent = []

                async def targets(_row):
                    return {"ok": True, "candidates": [
                        {"provider": "telegram", "recipient_id": "100"},
                        {"provider": "vk", "recipient_id": "200"},
                    ]}

                async def guard():
                    return {"ok": True}

                async def tg(_method, _payload):
                    return {"id": 100, "type": "private"}

                async def send(row, _text, _stage, _keyboard=None):
                    sent.append(row["target_source"])
                    if row["target_source"] == "telegram":
                        raise router.TelegramDeliveryError("bot was blocked", definitive=True)
                    return ["vk-1"]

                try:
                    await router._init_db()
                    await router._upsert_order({
                        "source_record_id": 300, "order_id": "fallback-300", "paid_at": router._iso(),
                        "course_key": "dog", "course": "Собака", "tariff": "vip", "flow": {},
                    }, 12)
                    router._resolve_targets, router._webhook_guard, router._tg_call, router._send_order_text = targets, guard, tg, send
                    ids, target = await router._send_stage_with_fallback(
                        {"id": 1, "order_id": "fallback-300"}, "Привет", "welcome", dict(router.DEFAULT_SETTINGS),
                    )
                    self.assertEqual((ids, target["provider"], sent), (["vk-1"], "vk", ["telegram", "vk"]))
                finally:
                    router._resolve_targets, router._webhook_guard, router._tg_call, router._send_order_text = originals
                    router._db_path = previous_db

        asyncio.run(run())

    def test_partial_telegram_message_is_never_treated_as_safe_fallback(self):
        async def run():
            original = router._tg_call
            calls = 0

            async def tg(_method, _payload):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return {"message_id": 10}
                raise router.TelegramDeliveryError("rejected", definitive=True)

            try:
                router._tg_call = tg
                with self.assertRaises(router.TelegramDeliveryError) as caught:
                    await router._send_text("100", "A" * 5000)
                self.assertFalse(caught.exception.definitive)
                self.assertIn("принял 1", str(caught.exception))
            finally:
                router._tg_call = original

        asyncio.run(run())

    def test_templates_reject_missing_required_and_unknown_variables(self):
        missing = router._template_validation_error("manager", "Только {video_instruction_url} и {vk_link}")
        unknown = router._template_validation_error("yes_reply", "Готово {secret_value}")
        self.assertIn("обязательн", missing.casefold())
        self.assertIn("неизвестные", unknown.casefold())

    def test_email_body_remains_plain_text_for_getcourse_pre_line_template(self):
        body = "Привет!\n\nОткройте https://example.com/a?x=1\nСпасибо"
        self.assertEqual(body.count("\n"), 3)
        self.assertNotIn("<br>", body)

    def test_health_is_honest_and_telegram_outage_is_only_degraded(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, previous_auth = router._db_path, router._require_admin
                previous_worker = dict(router._worker_state)
                previous_flow = dict(router._last_flow_result)
                previous_guard = dict(router._last_guard_result)
                router._db_path = Path(directory) / "module.db"

                async def allow(_request):
                    return {"role": "admin"}

                try:
                    await router._init_db()
                    now = router._iso()
                    await router._setting_updates({
                        "initialized": "1", "enabled": "1", "delivery_mode": "live",
                        "last_sync_success_at": now, "last_delivery_success_at": now,
                    })
                    router._require_admin = allow
                    router._worker_state.update({
                        "sync_success_at": now, "sync_error": "",
                        "delivery_success_at": now, "delivery_error": "",
                    })
                    router._last_flow_result.update({"ok": True, "status": "ready", "items": 90})
                    router._last_guard_result.update({"ok": False, "status": "error", "error": "Telegram offline"})
                    degraded = await router.health(object())
                    self.assertTrue(degraded["ok"])
                    self.assertEqual(degraded["status"], "degraded")
                    self.assertEqual(degraded["channels"]["vk"], "available_on_demand")

                    router._worker_state["sync_error"] = "source failed"
                    failed = await router.health(object())
                    self.assertFalse(failed["ok"])
                    self.assertEqual((failed["status"], failed["error"]), ("error", "source failed"))
                finally:
                    router._worker_state.clear()
                    router._worker_state.update(previous_worker)
                    router._last_flow_result.clear()
                    router._last_flow_result.update(previous_flow)
                    router._last_guard_result.clear()
                    router._last_guard_result.update(previous_guard)
                    router._require_admin = previous_auth
                    router._db_path = previous_db

        asyncio.run(run())

    def test_backfill_uses_moscow_day_and_archive_status(self):
        async def run():
            calls = []

            class Source:
                async def service_paid_course_orders(self, **kwargs):
                    return {
                        "ok": True, "cursor": 3, "max_source_record_id": 3,
                        "items": [
                            {"source_record_id": 1, "order_id": "before", "course_key": "dog", "paid_at": "2026-08-09T20:59:59Z"},
                            {"source_record_id": 2, "order_id": "inside", "course_key": "dog", "paid_at": "2026-08-09T21:00:00Z"},
                            {"source_record_id": 3, "order_id": "after", "course_key": "dog", "paid_at": "2026-08-10T21:00:00Z"},
                        ],
                    }

            original_module, original_settings, original_upsert = router._module, router._settings, router._upsert_order
            router._module = lambda *_: Source()
            router._settings = lambda: _async_value({**router.DEFAULT_SETTINGS, "reminder_hours": "12"})

            async def upsert(item, hours, *, initial_status=""):
                calls.append((item["order_id"], hours, initial_status))

            router._upsert_order = upsert
            try:
                result = await router._backfill_day("2026-08-10")
            finally:
                router._module, router._settings, router._upsert_order = original_module, original_settings, original_upsert
            self.assertEqual(calls, [("inside", 12, "backfill_only")])
            self.assertEqual((result["matched"], result["stored"]), (1, 1))

        async def _async_value(value):
            return value

        asyncio.run(run())

    def test_active_test_flow_prefers_latest_started_stream(self):
        items = [
            {"course_key": "dog", "stream": "60", "date_start": "2026-07-01", "vk_link": "v60", "tg_link": "t60"},
            {"course_key": "dog", "stream": "61", "date_start": "2026-08-01", "vk_link": "v61", "tg_link": "t61"},
            {"course_key": "dog", "stream": "62", "date_start": "2099-01-01", "vk_link": "v62", "tg_link": "t62"},
        ]
        self.assertEqual(router._active_test_flow(items, "dog")["stream"], "61")

    def test_vk_test_keyboard_contains_real_callback_actions(self):
        keyboard = json.loads(router._vk_test_keyboard("buttons-request-1"))
        self.assertTrue(keyboard["inline"])
        actions = [row[0]["action"] for row in keyboard["buttons"]]
        self.assertEqual([action["type"] for action in actions], ["callback", "callback"])
        self.assertEqual([action["label"] for action in actions], ["Да, курс открылся", "Нет, нужна помощь"])
        self.assertEqual([action["payload"]["answer"] for action in actions], ["yes", "no"])
        live = json.loads(router._vk_test_keyboard("buttons-request-live", "onboarding_live_test_response"))
        self.assertEqual(live["buttons"][1][0]["action"]["payload"]["command"], "onboarding_live_test_response")

    def test_telegram_test_keyboard_contains_real_https_buttons(self):
        keyboard = router._telegram_test_keyboard(
            "https://junior.sobakovod.pro/nexus", "tg-live-request-1234567890abcdef"
        )
        buttons = [row[0] for row in keyboard["inline_keyboard"]]
        self.assertEqual([button["text"] for button in buttons], ["Да, курс открылся", "Нет, нужна помощь"])
        self.assertTrue(all(button["url"].startswith("https://junior.sobakovod.pro/nexus/getcourse-onboarding/api/test/telegram/respond/") for button in buttons))
        self.assertTrue(buttons[0]["url"].endswith("?choice=yes"))
        self.assertTrue(buttons[1]["url"].endswith("?choice=no"))

    def test_real_reminder_buttons_are_callbacks_for_salebot(self):
        keyboard = router._reminder_keyboard()
        buttons = [row[0] for row in keyboard["inline_keyboard"]]
        self.assertEqual(
            [button["callback_data"] for button in buttons],
            ["onboarding_access_yes", "onboarding_access_help"],
        )
        self.assertFalse(any("url" in button for button in buttons))

    def test_vk_order_delivery_keeps_real_callback_keyboard(self):
        async def run():
            previous_module = router._module
            calls = []

            class Messenger:
                @staticmethod
                async def service_send_transfer_message(**kwargs):
                    calls.append(kwargs)
                    return {"ok": True, "message_id": 991}

            try:
                router._module = lambda *_: Messenger()
                row = {"id": 42, "target_source": "vk", "target_platform_id": "123"}
                keyboard = router._vk_test_keyboard("42", "onboarding_order_response")
                ids = await router._send_order_text(row, "Проверка", "reminder", keyboard)
                self.assertEqual(ids, ["991"])
                self.assertEqual(calls[0]["provider"], "vk")
                self.assertEqual(calls[0]["keyboard"], keyboard)
            finally:
                router._module = previous_module

        asyncio.run(run())

    def test_salebot_help_creates_one_task_and_returns_reply(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                amo_calls = []

                class Messenger:
                    @staticmethod
                    async def service_resolve_onboarding_telegram_target(**_kwargs):
                        return {"ok": True, "platform_id": "5601500901", "source": "salebot_id"}

                class Amo:
                    @staticmethod
                    async def service_create_onboarding_support_task(**kwargs):
                        amo_calls.append(kwargs)
                        return {"ok": True, "status": "created", "lead_id": "17759125", "task_id": "555"}

                class Incoming:
                    method = "POST"
                    query_params = {}
                    headers = {"content-type": "application/x-www-form-urlencoded"}

                    def __init__(self, secret):
                        self._secret = secret

                    async def body(self):
                        return f"secret={self._secret}&client_id=88001".encode()

                try:
                    await router._init_db()
                    await router._upsert_order(
                        {
                            "source_record_id": 7, "order_id": "paid-7", "deal_number": "7", "gc_user_id": "9",
                            "name": "Ирина", "email": "i@example.test", "phone": "+79990000000",
                            "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                            "tariff": "vip", "autopayment": "1", "manager_name": "", "utm_term": "salebot_id=88001",
                            "flow": {"stream": "61", "vk_link": "https://vk.test/61", "tg_link": "https://t.me/61"},
                        },
                        12,
                    )
                    db = await router._connect()
                    try:
                        await db.execute(
                            "UPDATE orders SET status='awaiting_response',branch='autopay',target_platform_id='5601500901' WHERE order_id='paid-7'"
                        )
                        secret = (await (await db.execute("SELECT value FROM settings WHERE key='salebot_help_secret'")).fetchone())[0]
                        await db.commit()
                    finally:
                        await db.close()

                    def module(module_id, _service):
                        return Amo() if module_id == "getcourse-amocrm" else Messenger()

                    router._module = module
                    first = await router.salebot_help(Incoming(secret))
                    first_body = json.loads(first.body)
                    self.assertEqual(first.status_code, 200)
                    self.assertEqual((first_body["status"], first_body["amo_task_id"]), ("created", "555"))
                    self.assertIn("Поняли, поможем разобраться", first_body["reply_text"])
                    second = await router.salebot_help(Incoming(secret))
                    self.assertEqual(json.loads(second.body)["status"], "already_requested")
                    self.assertEqual(len(amo_calls), 1)
                    self.assertEqual(amo_calls[0]["order_id"], "paid-7")
                    self.assertEqual(amo_calls[0]["phone"], "+79990000000")
                    self.assertEqual(amo_calls[0]["email"], "i@example.test")
                    self.assertEqual(amo_calls[0]["utm_term"], "salebot_id=88001")
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_salebot_yes_adds_one_note_to_latest_deal(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                note_calls = []

                class Messenger:
                    @staticmethod
                    async def service_resolve_onboarding_telegram_target(**_kwargs):
                        return {"ok": True, "platform_id": "5601500901", "source": "salebot_id"}

                class Amo:
                    @staticmethod
                    async def service_add_onboarding_confirmation_note(**kwargs):
                        note_calls.append(kwargs)
                        return {"ok": True, "status": "created", "lead_id": "17759125", "note_id": "9001"}

                class Incoming:
                    method = "POST"
                    query_params = {}
                    headers = {"content-type": "application/x-www-form-urlencoded"}

                    def __init__(self, secret):
                        self._secret = secret

                    async def body(self):
                        return f"secret={self._secret}&client_id=88001".encode()

                try:
                    await router._init_db()
                    await router._upsert_order(
                        {
                            "source_record_id": 8, "order_id": "paid-8", "deal_number": "8", "gc_user_id": "10",
                            "name": "Ирина", "email": "i@example.test", "phone": "+79990000000",
                            "paid_at": "2026-08-12T10:00:00Z", "course_key": "dog", "course": "Собака",
                            "tariff": "premium", "autopayment": "1", "manager_name": "", "utm_term": "salebot_id=88001",
                            "flow": {"stream": "61", "vk_link": "https://vk.test/61", "tg_link": "https://t.me/61"},
                        },
                        12,
                    )
                    db = await router._connect()
                    try:
                        await db.execute(
                            "UPDATE orders SET status='awaiting_response',branch='autopay_premium',target_platform_id='5601500901' WHERE order_id='paid-8'"
                        )
                        secret = (await (await db.execute("SELECT value FROM settings WHERE key='salebot_help_secret'")).fetchone())[0]
                        await db.commit()
                    finally:
                        await db.close()

                    def module(module_id, _service):
                        return Amo() if module_id == "getcourse-amocrm" else Messenger()

                    router._module = module
                    first = await router.salebot_confirm(Incoming(secret))
                    first_body = json.loads(first.body)
                    self.assertEqual(first.status_code, 200)
                    self.assertEqual((first_body["status"], first_body["amo_note_id"]), ("created", "9001"))
                    self.assertIn("доступ подтверждён", first_body["reply_text"].casefold())
                    second = await router.salebot_confirm(Incoming(secret))
                    self.assertEqual(json.loads(second.body)["status"], "already_confirmed")
                    self.assertEqual(len(note_calls), 1)
                    self.assertEqual(note_calls[0]["text"], "Пользователь подтвердил вход GetCourse")
                    db = await router._connect()
                    try:
                        stored = await (await db.execute(
                            "SELECT status,response,amo_lead_id,amo_note_id FROM orders WHERE order_id='paid-8'"
                        )).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(tuple(stored), ("confirmed", "yes", "17759125", "9001"))
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_vk_callback_sends_reply_once_without_amo_task(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                calls = []

                class Messenger:
                    @staticmethod
                    def _vk_group_id():
                        return "77"

                    @staticmethod
                    async def _vk_request(method, params):
                        calls.append((method, params))
                        return 991 if method == "messages.send" else 1

                class Incoming:
                    async def body(self):
                        return json.dumps(
                            {
                                "type": "message_event", "group_id": 77, "secret": "Secret123",
                                "object": {
                                    "user_id": 123, "peer_id": 123, "event_id": "event-1",
                                    "payload": router._vk_test_payload("buttons-request-1", "no"),
                                },
                            }
                        ).encode()

                try:
                    await router._init_db()
                    await router._setting_updates(
                        {"vk_test_callback_key": "callback-key", "vk_test_callback_secret": "Secret123"}
                    )
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                            ("buttons-request-1", "vk", "123", "sent", now, now),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    router._module = lambda *_: Messenger()
                    response = await router.vk_test_callback("callback-key", Incoming())
                    self.assertEqual(response.body, b"ok")
                    db = await router._connect()
                    try:
                        row = await (
                            await db.execute("SELECT status,results_json FROM test_runs WHERE request_id='buttons-request-1'")
                        ).fetchone()
                        amo_tasks = await (await db.execute("SELECT COUNT(*) FROM orders WHERE amo_task_id<>''")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["status"], "responded_no")
                    self.assertEqual(json.loads(row["results_json"])[0]["message_id"], 991)
                    self.assertEqual(amo_tasks[0], 0)
                    self.assertEqual([method for method, _ in calls], ["messages.sendMessageEventAnswer", "messages.send"])
                    await router.vk_test_callback("callback-key", Incoming())
                    self.assertEqual([method for method, _ in calls].count("messages.send"), 1)
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_vk_order_callback_claims_exact_order_once(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"

                class Messenger:
                    @staticmethod
                    def _vk_group_id():
                        return "77"

                    @staticmethod
                    async def _vk_request(_method, _params):
                        return 1

                class Incoming:
                    async def body(self):
                        return json.dumps({
                            "type": "message_event", "group_id": 77, "secret": "Secret123",
                            "object": {
                                "user_id": 123, "peer_id": 123, "event_id": "event-order-1",
                                "payload": router._vk_test_payload("1", "no", "onboarding_order_response"),
                            },
                        }).encode()

                try:
                    await router._init_db()
                    await router._setting_updates(
                        {"vk_test_callback_key": "callback-key", "vk_test_callback_secret": "Secret123"}
                    )
                    await router._upsert_order({
                        "source_record_id": 1, "order_id": "vk-order", "name": "VK ученик",
                        "paid_at": "2026-08-11T10:00:00Z", "course_key": "dog", "course": "Собака",
                        "tariff": "vip", "utm_term": "platform_id=123", "flow": {},
                    }, 12)
                    db = await router._connect()
                    try:
                        await db.execute(
                            "UPDATE orders SET status='awaiting_response',target_source='vk',target_platform_id='123' WHERE id=1"
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    router._module = lambda *_: Messenger()
                    self.assertEqual((await router.vk_test_callback("callback-key", Incoming())).body, b"ok")
                    self.assertEqual((await router.vk_test_callback("callback-key", Incoming())).body, b"ok")
                    db = await router._connect()
                    try:
                        row = await (await db.execute("SELECT response,status FROM orders WHERE id=1")).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual((row["response"], row["status"]), ("no", "response_pending"))
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_vk_button_send_registers_separate_event_callback(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                calls = []

                class Messenger:
                    @staticmethod
                    def _vk_group_id():
                        return "77"

                    @staticmethod
                    async def _vk_request(method, params):
                        calls.append((method, params))
                        if method == "groups.getCallbackConfirmationCode":
                            return {"code": "confirm-me"}
                        if method == "groups.getCallbackServers":
                            return {"items": []}
                        if method == "groups.addCallbackServer":
                            return {"server_id": 88}
                        return 7001 if method == "messages.send" else 1

                try:
                    await router._init_db()
                    router._module = lambda *_: Messenger()
                    result = await router._send_vk_button_test(
                        "123", "buttons-request-2", dict(router.DEFAULT_SETTINGS)
                    )
                    self.assertEqual(result["message_id"], 7001)
                    methods = [method for method, _ in calls]
                    self.assertEqual(
                        methods,
                        [
                            "groups.getCallbackConfirmationCode", "groups.getCallbackServers",
                            "groups.addCallbackServer", "groups.setCallbackSettings", "messages.send",
                        ],
                    )
                    callback_params = next(params for method, params in calls if method == "groups.addCallbackServer")
                    self.assertIn("/getcourse-onboarding/api/vk/callback/", callback_params["url"])
                    event_params = next(params for method, params in calls if method == "groups.setCallbackSettings")
                    self.assertEqual(event_params["message_event"], 1)
                    message_params = next(params for method, params in calls if method == "messages.send")
                    keyboard = json.loads(message_params["keyboard"])
                    self.assertEqual(keyboard["buttons"][0][0]["action"]["type"], "callback")
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_live_vk_no_button_creates_real_task_and_records_ids(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module = router._db_path, router._module
                router._db_path = Path(directory) / "module.db"
                vk_calls, amo_calls = [], []

                class Messenger:
                    @staticmethod
                    def _vk_group_id():
                        return "77"

                    @staticmethod
                    async def _vk_request(method, params):
                        vk_calls.append((method, params))
                        return 992 if method == "messages.send" else 1

                class Amo:
                    @staticmethod
                    async def service_create_onboarding_support_task(**kwargs):
                        amo_calls.append(kwargs)
                        return {"ok": True, "status": "created", "lead_id": "17759125", "task_id": "555"}

                class Incoming:
                    async def body(self):
                        return json.dumps(
                            {
                                "type": "message_event", "group_id": 77, "secret": "Secret123",
                                "object": {
                                    "user_id": 123, "peer_id": 123, "event_id": "event-live",
                                    "payload": router._vk_test_payload(
                                        "buttons-live-1", "no", "onboarding_live_test_response"
                                    ),
                                },
                            }
                        ).encode()

                try:
                    await router._init_db()
                    await router._setting_updates(
                        {"vk_test_callback_key": "callback-key", "vk_test_callback_secret": "Secret123"}
                    )
                    now = router._iso()
                    db = await router._connect()
                    try:
                        await db.execute(
                            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,status,results_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                            (
                                "buttons-live-1", "vk", "123", "sent",
                                json.dumps([{"mode": "live_task", "lead_id": "17759125", "order_id": "onboarding-live-test-buttons-live-1"}]),
                                now, now,
                            ),
                        )
                        await db.commit()
                    finally:
                        await db.close()

                    def module(module_id, _service):
                        return Amo() if module_id == "getcourse-amocrm" else Messenger()

                    router._module = module
                    response = await router.vk_test_callback("callback-key", Incoming())
                    self.assertEqual(response.body, b"ok")
                    self.assertEqual(len(amo_calls), 1)
                    self.assertEqual(amo_calls[0]["test_lead_id"], "17759125")
                    db = await router._connect()
                    try:
                        row = await (
                            await db.execute("SELECT status,results_json FROM test_runs WHERE request_id='buttons-live-1'")
                        ).fetchone()
                    finally:
                        await db.close()
                    results = json.loads(row["results_json"])
                    self.assertEqual(row["status"], "responded_no")
                    self.assertEqual(results[-1]["amo_task_id"], "555")
                    self.assertEqual([method for method, _ in vk_calls], ["messages.sendMessageEventAnswer", "messages.send"])
                finally:
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())

    def test_live_telegram_no_button_reuses_task_and_records_ids(self):
        async def run():
            with tempfile.TemporaryDirectory() as directory:
                previous_db, original_module, original_send = router._db_path, router._module, router._send_text
                router._db_path = Path(directory) / "module.db"
                amo_calls, telegram_calls = [], []
                request_id = "tg-live-request-1234567890abcdef"

                class Amo:
                    @staticmethod
                    async def service_create_onboarding_support_task(**kwargs):
                        amo_calls.append(kwargs)
                        return {"ok": True, "status": "existing", "lead_id": "17759125", "task_id": "14547499"}

                class Incoming:
                    async def body(self):
                        return b"choice=no"

                async def send(chat_id, text, keyboard=None):
                    telegram_calls.append((chat_id, text, keyboard))
                    return ["7001"]

                try:
                    await router._init_db()
                    now = router._iso()
                    metadata = [{
                        "mode": "telegram_live_task", "lead_id": "17759125",
                        "order_id": f"onboarding-live-test-{request_id}",
                        "task_reference": "vk-live-task-20260811-tehpod-v1",
                    }]
                    db = await router._connect()
                    try:
                        await db.execute(
                            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,status,results_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                            (request_id, "telegram", "5601500901", "sent", json.dumps(metadata), now, now),
                        )
                        await db.commit()
                    finally:
                        await db.close()
                    router._module = lambda *_: Amo()
                    router._send_text = send
                    response = await router.telegram_test_response_submit(request_id, Incoming())
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(len(amo_calls), 1)
                    self.assertEqual(amo_calls[0]["test_lead_id"], "17759125")
                    self.assertIn("vk-live-task-20260811-tehpod-v1", amo_calls[0]["text"])
                    self.assertEqual(telegram_calls[0][0], "5601500901")
                    self.assertIn("#14547499", telegram_calls[0][1])
                    db = await router._connect()
                    try:
                        row = await (
                            await db.execute("SELECT status,results_json FROM test_runs WHERE request_id=?", (request_id,))
                        ).fetchone()
                    finally:
                        await db.close()
                    self.assertEqual(row["status"], "responded_no")
                    self.assertEqual(json.loads(row["results_json"])[-1]["amo_task_id"], "14547499")
                finally:
                    router._send_text = original_send
                    router._module = original_module
                    router._db_path = previous_db

        asyncio.run(run())


class TelegramIdentityResolverTests(unittest.TestCase):
    def setUp(self):
        from module_messenger_widget.identity_graph import IdentityIndex

        self.directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.directory.name) / "customers.db"
        with sqlite3.connect(self.db_path) as db:
            db.execute("CREATE TABLE cdb_telegram_clients(platform_id TEXT,custom_fields TEXT)")
            db.executemany(
                "INSERT INTO cdb_telegram_clients(platform_id,custom_fields) VALUES(?,?)",
                [
                    ("10001", json.dumps({"name": "Прямой"})),
                    ("20002", json.dumps({"salebot_id": "88001"})),
                    ("30003", json.dumps({"possible_accounts": [{"salebot_client_id": "99001"}]})),
                ],
            )
        self.index = IdentityIndex(self.db_path, Path(self.directory.name) / "index.db")

    def tearDown(self):
        self.directory.cleanup()

    def test_platform_id_is_exact_senler_telegram_id(self):
        result = self.index.telegram_target_for_utm_term("platform_id=10001")
        self.assertTrue(result["ok"])
        self.assertEqual((result["platform_id"], result["source"]), ("10001", "senler_platform_id"))

    def test_salebot_id_bridges_to_telegram_platform_id(self):
        result = self.index.telegram_target_for_utm_term("salebot_id=88001")
        self.assertTrue(result["ok"])
        self.assertEqual((result["platform_id"], result["source"]), ("20002", "salebot_id"))
        nested = self.index.telegram_target_for_utm_term("salebot_id=99001")
        self.assertEqual(nested["platform_id"], "30003")

    def test_raw_id_conflict_is_blocked(self):
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO cdb_telegram_clients(platform_id,custom_fields) VALUES(?,?)",
                ("40004", json.dumps({"salebot_id": "10001"})),
            )
        result = self.index.telegram_target_for_utm_term("10001")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "conflict")


if __name__ == "__main__":
    unittest.main()
