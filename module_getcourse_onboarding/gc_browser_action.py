from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _money(value: Any) -> float:
    text = _clean(value, 100).replace("\xa0", " ").replace("руб.", "").replace("₽", "")
    text = re.sub(r"[^0-9,.-]", "", text).replace(",", ".")
    try:
        return round(float(text), 2)
    except (TypeError, ValueError):
        return -1


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)
    path.chmod(0o600)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _checkpoint(path: Path, journal: dict[str, Any], step: str, **details: Any) -> None:
    journal.update({"step": step, "updated_at": _now(), **details})
    _atomic_json(path, journal)


async def _authenticated(page, base_url: str) -> None:
    parsed = urlparse(page.url)
    if parsed.netloc != urlparse(base_url).netloc:
        raise RuntimeError(f"GetCourse открыл неожиданный домен: {parsed.netloc or 'пусто'}")
    blocked = ("/cms/system/login", "/pl/2fa", "/login")
    if any(marker in parsed.path for marker in blocked):
        raise RuntimeError("Сессия GetCourse истекла: требуется повторный вход администратора")
    if await page.locator('input[type="password"]').count() and await page.get_by_text("Войти", exact=True).count():
        raise RuntimeError("Сессия GetCourse истекла: открылась форма входа")


async def _goto(page, url: str, base_url: str) -> None:
    # The GetCourse order list can keep parsing third-party widgets for longer
    # than 30 seconds even though the authenticated admin document is already
    # available.  Waiting for DOMContentLoaded here turned a slow page into a
    # false "session lost" alert and paused upgrades.  Commit is enough to
    # verify the HTTP response and redirect target; the caller waits for the
    # exact page element it needs before making any change.
    response = await page.goto(url, wait_until="commit", timeout=30_000)
    if response is not None and response.status >= 400:
        raise RuntimeError(f"GetCourse вернул HTTP {response.status} для {urlparse(url).path}")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5_000)
    except Exception:
        # _authenticated below still catches a login redirect.  Do not reject
        # a valid but slow GetCourse admin page just because optional widgets
        # have delayed its DOMContentLoaded event.
        pass
    await _authenticated(page, base_url)
    await page.wait_for_timeout(500)


async def _screenshot(page, artifacts: Path, operation_id: str, suffix: str) -> str:
    artifacts.mkdir(parents=True, exist_ok=True)
    artifacts.chmod(0o700)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", operation_id)[:80] or "operation"
    target = artifacts / f"{safe}-{suffix}.png"
    await page.screenshot(path=str(target), full_page=True)
    target.chmod(0o600)
    return str(target)


def _payment_id(url: str) -> str:
    match = re.search(r"/payment/update/id/(\d+)", url)
    return match.group(1) if match else ""


def _order_id(url: str) -> str:
    match = re.search(r"/deal/update/id/(\d+)", url)
    return match.group(1) if match else ""


def _browser_executable() -> str | None:
    roots = []
    configured = _clean(os.environ.get("PLAYWRIGHT_BROWSERS_PATH"), 4000)
    if configured and configured != "0":
        roots.append(Path(configured))
    roots.append(Path.home() / ".cache" / "ms-playwright")
    candidates: list[Path] = []
    for root in roots:
        candidates.extend(root.glob("chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell"))
        candidates.extend(root.glob("chromium_headless_shell-*/chrome-linux/headless_shell"))
        candidates.extend(root.glob("chromium-*/chrome-linux/chrome"))
    candidates.extend(Path(item) for item in ("/usr/bin/google-chrome", "/usr/bin/chromium") if Path(item).is_file())
    existing = sorted((item for item in candidates if item.is_file()), reverse=True)
    return str(existing[0]) if existing else None


async def _verify_order_title(page, deal_number: str) -> None:
    heading = page.locator("h1").first
    await heading.wait_for(state="visible", timeout=15_000)
    title = _clean(await heading.inner_text(), 300)
    if f"#{deal_number}" not in title:
        raise RuntimeError(f"Открыт другой заказ: ожидался #{deal_number}, получено «{title}»")


async def _discover_payment(page, expected_amount: float) -> str:
    links = page.locator('a[href*="/sales/control/payment/update/id/"]')
    candidates: list[str] = []
    for index in range(await links.count()):
        link = links.nth(index)
        row = link.locator("xpath=ancestor::tr[1]")
        # The order history repeats the same payment link outside the payments
        # table.  Only a link contained in a real table row represents the
        # current ledger entry; positional selectors are unsafe here.
        if await row.count() != 1:
            continue
        row_text = _clean(await row.inner_text(), 2000)
        if "Получен" not in row_text:
            continue
        amounts = [_money(item) for item in re.findall(r"[0-9][0-9\s\xa0]*[.,]?[0-9]*\s*(?:руб\.|₽)", row_text)]
        if not any(abs(value - expected_amount) <= 0.01 for value in amounts):
            continue
        href = _clean(await link.get_attribute("href"), 1000)
        if href:
            candidates.append(href)
    candidates = list(dict.fromkeys(candidates))
    if len(candidates) != 1:
        raise RuntimeError(
            f"Нужен ровно один полученный платёж на {expected_amount:.2f} ₽; найдено: {len(candidates)}"
        )
    return candidates[0]


async def _payment_state(page, expected_amount: float) -> tuple[str, str]:
    amount = _money(await page.locator("#Payment_amount").input_value())
    if abs(amount - expected_amount) > 0.01:
        raise RuntimeError(f"Сумма платежа изменилась: ожидалось {expected_amount:.2f}, получено {amount:.2f}")
    deal_number = _clean(await page.locator("#Payment_dealNumber").input_value(), 100)
    target_href = _clean(
        await page.locator('a:has-text("Перейти к заказу")').first.get_attribute("href"), 1000
    )
    return deal_number, target_href


async def _order_state(
    page,
    *,
    expected_offer_id: str,
    expected_cost: float,
    expected_received: float,
) -> dict[str, Any]:
    offer = page.locator(
        f'.positions-table a[href="/pl/sales/offer/update?id={expected_offer_id}"]'
    )
    all_offer_links = page.locator('.positions-table a[href*="/pl/sales/offer/update?id="]')
    if await offer.count() != 1 or await all_offer_links.count() != 1:
        raise RuntimeError("Состав заказа не совпадает с ожидаемым единственным предложением")
    prices = page.locator('.positions-table input[id$="_price"]')
    price_count = await prices.count()
    if price_count == 1:
        cost = _money(await prices.first.input_value())
    elif price_count == 0:
        # Completed orders are read-only and render the total as text instead
        # of keeping the position price input in the DOM.
        total_label = page.get_by_text("Сумма заказа", exact=True)
        if await total_label.count() != 1:
            raise RuntimeError("GetCourse не показал однозначную сумму заказа")
        total_text = _clean(
            await total_label.locator("xpath=ancestor::tr[1]").inner_text(), 500
        )
        amounts = [_money(item) for item in re.findall(
            r"[0-9][0-9\s\xa0]*[.,]?[0-9]*\s*(?:руб\.|₽)", total_text
        )]
        cost = expected_cost if any(abs(value - expected_cost) <= 0.01 for value in amounts) else -1
    else:
        raise RuntimeError("В заказе ожидалась ровно одна позиция")
    if abs(cost - expected_cost) > 0.01:
        raise RuntimeError(f"Стоимость заказа изменилась: ожидалось {expected_cost:.2f}, получено {cost:.2f}")

    status_label = page.get_by_text("Статус:", exact=True)
    if await status_label.count() != 1:
        raise RuntimeError("GetCourse не показал однозначный статус заказа")
    status_text = _clean(
        await status_label.locator("xpath=ancestor::tr[1]").inner_text(), 300
    ).replace("Статус:", "").strip()

    received: list[str] = []
    links = page.locator('a[href*="/sales/control/payment/update/id/"]')
    for index in range(await links.count()):
        link = links.nth(index)
        row = link.locator("xpath=ancestor::tr[1]")
        if await row.count() != 1:
            continue
        row_text = _clean(await row.inner_text(), 2000)
        if "Получен" not in row_text:
            continue
        amounts = [_money(item) for item in re.findall(
            r"[0-9][0-9\s\xa0]*[.,]?[0-9]*\s*(?:руб\.|₽)", row_text
        )]
        if any(abs(value - expected_received) <= 0.01 for value in amounts):
            href = _clean(await link.get_attribute("href"), 1000)
            if href:
                received.append(href)
    received = list(dict.fromkeys(received))
    if expected_received <= 0 and received:
        raise RuntimeError("В заказе неожиданно найден полученный платёж")
    if expected_received > 0 and len(received) != 1:
        raise RuntimeError(
            f"В заказе ожидался один полученный платёж на {expected_received:.2f} ₽; найдено: {len(received)}"
        )
    return {
        "status": status_text,
        "offer_id": expected_offer_id,
        "cost": expected_cost,
        "received": expected_received,
        "payment_id": _payment_id(urljoin(page.url, received[0])) if received else "",
    }


async def _inspect_order(page, payload: dict[str, Any], base_url: str, artifacts: Path) -> dict[str, Any]:
    order_id = _clean(payload.get("order_id"), 100)
    deal_number = _clean(payload.get("deal_number"), 100)
    offer_id = _clean(payload.get("offer_id"), 30)
    expected_status = _clean(payload.get("expected_status"), 100).casefold()
    expected_cost = round(float(payload.get("expected_cost") or 0), 2)
    expected_received = round(float(payload.get("expected_received") or 0), 2)
    if not order_id.isdigit() or not deal_number or not offer_id.isdigit() or expected_cost < 0 or expected_received < 0:
        raise RuntimeError("Для проверки заказа переданы некорректные реквизиты")
    await _goto(page, urljoin(base_url, f"/sales/control/deal/update/id/{order_id}"), base_url)
    await _verify_order_title(page, deal_number)
    state = await _order_state(
        page,
        expected_offer_id=offer_id,
        expected_cost=expected_cost,
        expected_received=expected_received,
    )
    if expected_status and state["status"].casefold() != expected_status:
        raise RuntimeError(
            f"Статус заказа не совпадает: ожидался «{expected_status}», получено «{state['status']}»"
        )
    proof = await _screenshot(page, artifacts, _clean(payload.get("operation_id"), 100), "order")
    return {"ok": True, "status": "inspected", "order_id": order_id, "deal_number": deal_number, "proof": proof, **state}


async def _cancel_order(page, payload: dict[str, Any], base_url: str, artifacts: Path) -> dict[str, Any]:
    checked = await _inspect_order(page, {**payload, "expected_status": ""}, base_url, artifacts)
    if checked["status"].casefold() == "отменен":
        return {**checked, "status": "cancelled", "already_cancelled": True}
    if checked["status"].casefold() not in {"новый", "в работе"}:
        raise RuntimeError(f"Автоматическая отмена запрещена для статуса «{checked['status']}»")
    cancel = page.locator('a.btn-change-status[data-status="cancelled"]')
    if await cancel.count() != 1:
        raise RuntimeError("GetCourse не показал однозначное действие «Отменен»")
    await cancel.click()
    await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(1_000)
    await _authenticated(page, base_url)
    verified = await _inspect_order(
        page, {**payload, "expected_status": "Отменен"}, base_url, artifacts
    )
    return {**verified, "status": "cancelled", "already_cancelled": False}


async def _complete_order(page, payload: dict[str, Any], base_url: str, artifacts: Path) -> dict[str, Any]:
    checked = await _inspect_order(page, {**payload, "expected_status": ""}, base_url, artifacts)
    if checked["status"].casefold() == "завершен":
        return {**checked, "status": "completed", "already_completed": True}
    if checked["status"].casefold() not in {"новый", "в работе"}:
        raise RuntimeError(f"Автоматическое завершение запрещено для статуса «{checked['status']}»")
    expected_cost = round(float(payload.get("expected_cost") or 0), 2)
    expected_received = round(float(payload.get("expected_received") or 0), 2)
    if expected_received <= 0 or abs(expected_cost - expected_received) > 0.01:
        raise RuntimeError("Завершать можно только полностью оплаченный заказ")
    selector = page.locator("#Deal_change_status")
    if await selector.count() != 1:
        raise RuntimeError("GetCourse не показал однозначный переключатель статуса заказа")
    option = selector.locator('option[value="payed"]')
    if await option.count() != 1 or _clean(await option.inner_text(), 100) != "Завершен":
        raise RuntimeError("В переключателе нет ожидаемого статуса «Завершен»")
    # GetCourse attaches a legacy change handler to the select.  Selecting via
    # the normal DOM event can start a partial navigation before the form is
    # saved, so set the exact native value and submit the existing deal form.
    await selector.evaluate("element => { element.value = 'payed'; }")
    if await selector.input_value() != "payed":
        raise RuntimeError("GetCourse не принял выбранный статус «Завершен»")
    save = page.locator('#dealForm button[name="save"]')
    if await save.count() != 1:
        raise RuntimeError("GetCourse не показал однозначную кнопку сохранения заказа")
    await save.click()
    await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(1_000)
    await _authenticated(page, base_url)
    verified = await _inspect_order(
        page, {**payload, "expected_status": "Завершен"}, base_url, artifacts
    )
    return {**verified, "status": "completed", "already_completed": False}


async def _inspect_access(page, payload: dict[str, Any], base_url: str, artifacts: Path) -> dict[str, Any]:
    user_id = _clean(payload.get("gc_user_id"), 100)
    course_key = _clean(payload.get("course_key"), 20)
    if not user_id.isdigit() or course_key not in {"puppy", "dog", "combo"}:
        raise RuntimeError("Для проверки доступов нужны числовой gc_user_id и известный курс")
    await _goto(page, urljoin(base_url, f"/user/control/user/update/id/{user_id}"), base_url)
    panel = page.locator("#userFormGroups")
    if await panel.count() != 1:
        raise RuntimeError("GetCourse не показал однозначный блок текущих групп пользователя")
    lines = {
        _clean(line, 500).replace("ё", "е").casefold()
        for line in (await panel.inner_text()).splitlines()
        if _clean(line, 500)
    }
    courses = ("puppy", "dog") if course_key == "combo" else (course_key,)
    expected = []
    forbidden = []
    for course in courses:
        label = "щенок" if course == "puppy" else "собака"
        expected.append(f"премиум. {label}")
        forbidden.append(f"стандарт. {label}")
    missing = [name for name in expected if name not in lines]
    present_forbidden = [name for name in forbidden if name in lines]
    if missing or present_forbidden:
        raise RuntimeError(
            "Доступы ещё не переключены: "
            + (f"нет {', '.join(missing)}" if missing else "")
            + (f"; остались {', '.join(present_forbidden)}" if present_forbidden else "")
        )
    proof = await _screenshot(page, artifacts, _clean(payload.get("operation_id"), 100), "access")
    return {
        "ok": True,
        "status": "access_ready",
        "gc_user_id": user_id,
        "course_key": course_key,
        "required_groups": expected,
        "forbidden_groups": forbidden,
        "proof": proof,
    }


async def _read_access(page, payload: dict[str, Any], base_url: str) -> dict[str, Any]:
    """Read exact checked group ids from the authenticated user editor."""

    user_id = _clean(payload.get("gc_user_id"), 100)
    if not user_id.isdigit():
        raise RuntimeError("Для проверки доступов нужен числовой gc_user_id")
    await _goto(
        page,
        urljoin(base_url, f"/user/control/user/update?id={user_id}&part=groups"),
        base_url,
    )
    available = page.locator('input[name="User[groupIds][]"]')
    await available.first.wait_for(state="attached", timeout=15_000)
    selected = page.locator('input[name="User[groupIds][]"]:checked')
    groups: list[dict[str, str]] = []
    for index in range(await selected.count()):
        checkbox = selected.nth(index)
        group_id = _clean(await checkbox.input_value(), 30)
        label = checkbox.locator("xpath=parent::label[1]")
        name = _clean(await label.inner_text(), 500) if await label.count() == 1 else ""
        if group_id.isdigit() and name:
            groups.append({"group_id": group_id, "name": name})
    return {
        "ok": True,
        "status": "access_read",
        "gc_user_id": user_id,
        "groups": groups,
        "checked_at": _now(),
    }


async def _recalculate(page, base_url: str, target_url: str, target_deal_number: str, expected_amount: float) -> str:
    await _goto(page, urljoin(base_url, target_url), base_url)
    await _verify_order_title(page, target_deal_number)
    order_id = _order_id(page.url)
    if not order_id:
        raise RuntimeError("GetCourse не вернул внутренний ID нового Premium-заказа")
    payment_links = page.locator('a[href*="/sales/control/payment/update/id/"]')
    matching = []
    for index in range(await payment_links.count()):
        link = payment_links.nth(index)
        row = link.locator("xpath=ancestor::tr[1]")
        if await row.count() != 1:
            continue
        row_text = _clean(await row.inner_text(), 2000)
        if "Получен" in row_text and any(
            abs(_money(item) - expected_amount) <= 0.01
            for item in re.findall(r"[0-9][0-9\s\xa0]*[.,]?[0-9]*\s*(?:руб\.|₽)", row_text)
        ):
            matching.append(link)
    if len(matching) != 1:
        raise RuntimeError(f"В новом заказе найдено платежей нужной суммы: {len(matching)}")

    await page.get_by_role("button", name=re.compile("Действия")).click()
    action = page.locator('a[data-action="recalc_commissions"]:has-text("Пересчитать платежи")').first
    await action.wait_for(state="visible", timeout=10_000)
    dialog_message = {"text": ""}

    async def accept_dialog(dialog):
        dialog_message["text"] = _clean(dialog.message, 500)
        await dialog.accept()

    page.once("dialog", accept_dialog)
    await action.click()
    await page.wait_for_timeout(1200)
    await page.reload(wait_until="domcontentloaded", timeout=30_000)
    await _authenticated(page, base_url)
    await _verify_order_title(page, target_deal_number)
    body = _clean(await page.locator("body").inner_text(), 100_000)
    normalized = body.replace("\xa0", " ")
    expected_text = f"{expected_amount:,.0f}".replace(",", " ") + " руб."
    if "Оплачено пользователем" not in normalized or expected_text not in normalized:
        raise RuntimeError("После пересчёта новый заказ не показывает ожидаемую оплаченную сумму")
    return order_id


async def _probe(page, base_url: str) -> dict[str, Any]:
    await _goto(page, urljoin(base_url, "/sales/control/deal/index"), base_url)
    if not await page.get_by_text("Заказы", exact=True).count() and "заказ" not in (await page.title()).casefold():
        raise RuntimeError("GetCourse открылся, но раздел заказов не распознан")
    return {"ok": True, "authenticated": True, "url": page.url, "checked_at": _now()}


async def _inspect_payment(page, payload: dict[str, Any], base_url: str) -> dict[str, Any]:
    source_order_id = _clean(payload.get("source_order_id"), 100)
    source_deal_number = _clean(payload.get("source_deal_number"), 100)
    expected_amount = round(float(payload.get("expected_amount") or 0), 2)
    if not re.fullmatch(r"\d+", source_order_id) or not source_deal_number or expected_amount <= 0:
        raise RuntimeError("Для проверки нужны ID, номер исходного заказа и положительная сумма")
    source_url = urljoin(base_url, f"/sales/control/deal/update/id/{source_order_id}")
    await _goto(page, source_url, base_url)
    await _verify_order_title(page, source_deal_number)
    payment_url = urljoin(base_url, await _discover_payment(page, expected_amount))
    await _goto(page, payment_url, base_url)
    current_deal, target_url = await _payment_state(page, expected_amount)
    if current_deal != source_deal_number:
        raise RuntimeError(f"Платёж связан с заказом #{current_deal}, а ожидался #{source_deal_number}")
    return {
        "ok": True,
        "status": "inspected",
        "payment_id": _payment_id(payment_url),
        "source_deal_number": current_deal,
        "source_order_id": _order_id(urljoin(base_url, target_url)),
        "amount": expected_amount,
        "checked_at": _now(),
    }


async def _transfer(page, payload: dict[str, Any], base_url: str, journal_path: Path, artifacts: Path) -> dict[str, Any]:
    operation_id = _clean(payload.get("operation_id"), 100)
    source_order_id = _clean(payload.get("source_order_id"), 100)
    source_deal_number = _clean(payload.get("source_deal_number"), 100)
    target_deal_number = _clean(payload.get("target_deal_number"), 100)
    expected_amount = round(float(payload.get("expected_amount") or 0), 2)
    if not re.fullmatch(r"\d+", source_order_id):
        raise RuntimeError("Внутренний ID исходного заказа должен быть числовым")
    if not source_deal_number or not target_deal_number or source_deal_number == target_deal_number:
        raise RuntimeError("Номера исходного и целевого заказов должны быть разными")
    if expected_amount <= 0:
        raise RuntimeError("Ожидаемая сумма платежа должна быть больше нуля")

    journal = _load_json(journal_path)
    if journal and _clean(journal.get("operation_id"), 100) != operation_id:
        raise RuntimeError("Журнал браузерной операции принадлежит другой доплате")
    journal.setdefault("operation_id", operation_id)
    journal.setdefault("source_order_id", source_order_id)
    journal.setdefault("source_deal_number", source_deal_number)
    journal.setdefault("target_deal_number", target_deal_number)
    journal.setdefault("expected_amount", expected_amount)
    _checkpoint(journal_path, journal, journal.get("step") or "started")

    payment_url = _clean(journal.get("payment_url"), 1000)
    if not payment_url:
        source_url = urljoin(base_url, f"/sales/control/deal/update/id/{source_order_id}")
        await _goto(page, source_url, base_url)
        await _verify_order_title(page, source_deal_number)
        payment_url = urljoin(base_url, await _discover_payment(page, expected_amount))
        _checkpoint(
            journal_path,
            journal,
            "payment_discovered",
            payment_url=payment_url,
            payment_id=_payment_id(payment_url),
        )

    await _goto(page, payment_url, base_url)
    current_deal, target_url = await _payment_state(page, expected_amount)
    if current_deal not in {source_deal_number, target_deal_number}:
        raise RuntimeError(
            f"Платёж уже связан с неожиданным заказом #{current_deal}; автоматическая работа остановлена"
        )
    if current_deal == source_deal_number:
        await page.locator("#Payment_dealNumber").fill(target_deal_number)
        for selector in ("#ParamsObject_notify_user", "#ParamsObject_notify_admin"):
            checkbox = page.locator(selector)
            if await checkbox.count() and await checkbox.is_checked():
                await checkbox.uncheck()
        await page.get_by_role("button", name="Сохранить", exact=True).click()
        # GetCourse may submit this legacy form asynchronously.  Waiting for
        # the current load state alone can resolve before the request starts;
        # give the submission time to leave the page before reopening it for
        # the authoritative persisted-value check.
        await page.wait_for_timeout(2_000)
        confirm = page.get_by_role("button", name="Перенести платёж", exact=True)
        if await confirm.count():
            warning = page.get_by_text(
                "Вы уверены, что хотите перенести платеж? По одному из платежей заказа уже выбит чек полной оплаты",
                exact=False,
            )
            if await warning.count() != 1 or await confirm.count() != 1:
                raise RuntimeError("GetCourse показал неизвестное подтверждение переноса платежа")
            await confirm.click()
            await page.wait_for_timeout(2_000)
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
        await _authenticated(page, base_url)
        await _goto(page, payment_url, base_url)
        current_deal, target_url = await _payment_state(page, expected_amount)
        if current_deal != target_deal_number:
            raise RuntimeError("GetCourse не сохранил перенос платежа в новый заказ")
    if not target_url:
        raise RuntimeError("После переноса платежа отсутствует ссылка на целевой заказ")
    _checkpoint(
        journal_path,
        journal,
        "payment_moved",
        payment_url=payment_url,
        payment_id=_payment_id(payment_url),
        target_order_url=urljoin(base_url, target_url),
    )

    target_order_id = await _recalculate(
        page, base_url, urljoin(base_url, target_url), target_deal_number, expected_amount
    )
    proof = await _screenshot(page, artifacts, operation_id, "recalculated")
    _checkpoint(
        journal_path,
        journal,
        "recalculated",
        target_order_id=target_order_id,
        proof=proof,
        completed_at=_now(),
    )
    return {
        "ok": True,
        "status": "recalculated",
        "payment_id": _payment_id(payment_url),
        "payment_url": payment_url,
        "target_order_id": target_order_id,
        "target_order_url": urljoin(base_url, target_url),
        "proof": proof,
        "journal": str(journal_path),
    }


async def _run(payload: dict[str, Any]) -> dict[str, Any]:
    from playwright.async_api import async_playwright

    action = _clean(payload.get("action"), 40)
    base_url = _clean(payload.get("base_url"), 1000).rstrip("/")
    storage_state = Path(_clean(payload.get("storage_state"), 4000))
    journal_path = Path(_clean(payload.get("journal_path"), 4000))
    artifacts = Path(_clean(payload.get("artifacts_dir"), 4000))
    operation_id = _clean(payload.get("operation_id"), 100) or "probe"
    if base_url != "https://club.sobakovod.pro":
        raise RuntimeError("Разрешён только административный домен club.sobakovod.pro")
    if not storage_state.is_file():
        raise RuntimeError("Сессия GetCourse не загружена")
    artifacts.mkdir(parents=True, exist_ok=True)
    artifacts.chmod(0o700)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            executable_path=_browser_executable(),
            args=["--disable-dev-shm-usage", "--no-sandbox", "--renderer-process-limit=2"],
        )
        context = await browser.new_context(storage_state=str(storage_state), locale="ru-RU")
        page = await context.new_page()
        page.set_default_timeout(15_000)
        try:
            if action == "probe":
                return await _probe(page, base_url)
            if action == "inspect_payment":
                return await _inspect_payment(page, payload, base_url)
            if action == "inspect_order":
                return await _inspect_order(page, payload, base_url, artifacts)
            if action == "cancel_order":
                return await _cancel_order(page, payload, base_url, artifacts)
            if action == "inspect_access":
                return await _inspect_access(page, payload, base_url, artifacts)
            if action == "read_access":
                return await _read_access(page, payload, base_url)
            if action == "complete_order":
                return await _complete_order(page, payload, base_url, artifacts)
            if action == "transfer_payment":
                return await _transfer(page, payload, base_url, journal_path, artifacts)
            raise RuntimeError(f"Неизвестное браузерное действие: {action}")
        except Exception as exc:
            proof = ""
            try:
                proof = await _screenshot(page, artifacts, operation_id, "error")
            except Exception:
                pass
            return {
                "ok": False,
                "status": "error",
                "error": _clean(exc, 1500),
                "url": _clean(page.url, 2000),
                "proof": proof,
                "journal": str(journal_path),
                "failed_at": _now(),
            }
        finally:
            await context.close()
            await browser.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", required=True)
    args = parser.parse_args()
    payload_path = Path(args.payload)
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        result = asyncio.run(_run(payload))
    except Exception as exc:
        result = {"ok": False, "status": "fatal", "error": _clean(exc, 1500), "failed_at": _now()}
    sys.stdout.write(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
