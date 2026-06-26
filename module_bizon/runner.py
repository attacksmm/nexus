from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

MODERATOR_LOGIN_URL = "https://start.bizon365.ru/my/login"
MODERATOR_CHECK_URL = "https://start.bizon365.ru/my/webinars"

ROOM_OPEN_DELAY_S = 1
RECONNECT_DELAY_S = 30
PAGE_TIMEOUT_MS = 60000
READY_TIMEOUT_MS = 60000
HEARTBEAT_INTERVAL_S = 300

_stop_requested = False
log = logging.getLogger("bizon-runner")


class ActivityTracker:
    def __init__(self) -> None:
        self._last = time.monotonic()

    def touch(self) -> None:
        self._last = time.monotonic()

    def seconds_since(self) -> float:
        return time.monotonic() - self._last


class ReplyHealthTracker:
    def __init__(self) -> None:
        self.queued = 0
        self.replied = 0
        self.last_queue_at: float | None = None
        self.last_reply_at: float | None = None
        self.last_alert_at = 0.0

    def mark_queued(self) -> None:
        self.queued += 1
        self.last_queue_at = time.monotonic()

    def mark_replied(self) -> None:
        self.replied += 1
        self.last_reply_at = time.monotonic()

    def seconds_since_last_reply_after_queue(self) -> float | None:
        if self.last_queue_at is None:
            return None
        if self.last_reply_at is not None and self.last_reply_at >= self.last_queue_at:
            return None
        return time.monotonic() - self.last_queue_at


activity = ActivityTracker()
reply_health = ReplyHealthTracker()
_console_alert_last: dict[str, float] = {}
_runtime_cfg: dict[str, Any] = {}
_instance_lock_handle: Any | None = None


def _on_signal(signum, _frame) -> None:
    global _stop_requested
    _stop_requested = True
    log.warning("signal %s received, stopping after current cleanup", signum)


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [bizon] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    try:
        stdout_stat = os.fstat(sys.stdout.fileno())
        log_stat = log_file.stat() if log_file.exists() else None
        stdout_is_log = bool(log_stat and stdout_stat.st_ino == log_stat.st_ino and stdout_stat.st_dev == log_stat.st_dev)
    except Exception:
        stdout_is_log = False
    if not stdout_is_log:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)


def acquire_instance_lock(config_path: Path) -> bool:
    global _instance_lock_handle
    lock_path = config_path.parent / "bizon-runner.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        owner = handle.read().strip() or "unknown"
        print(f"Bizon runner is already active (pid={owner})", file=sys.stderr)
        handle.close()
        return False
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    _instance_lock_handle = handle
    return True


def _clean_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.replace("\r", "\n").replace(",", "\n").split("\n") if part.strip()]
    return []


def load_config(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    base_url = str(raw.get("base_url") or "").strip().rstrip("/")
    sec_key = str(raw.get("sec_key") or "").strip()
    rooms = _clean_list(raw.get("rooms"))
    login = str(raw.get("login") or os.environ.get("BIZON365_LOGIN") or "").strip()
    password = str(raw.get("password") or os.environ.get("BIZON365_PASS") or "").strip()
    if not login or not password:
        raise RuntimeError("BIZON365 login/password are not configured")
    if not base_url or not sec_key or not rooms:
        raise RuntimeError("base_url, sec_key and rooms are required")

    supervisor = raw.get("supervisor") if isinstance(raw.get("supervisor"), dict) else {}
    urls = [f"{base_url}/{room}?sec={sec_key}" for room in rooms]
    return {
        "login": login,
        "password": password,
        "base_url": base_url,
        "sec_key": sec_key,
        "rooms": rooms,
        "urls": urls,
        "profile_dir": Path(raw.get("profile_dir") or path.parent / "profile"),
        "log_file": Path(raw.get("log_file") or path.parent / "logs" / "module.log"),
        "scheduled_restart_time": str(supervisor.get("scheduled_restart_time") or "03:30"),
        "silence_windows": supervisor.get("silence_windows") if isinstance(supervisor.get("silence_windows"), list) else [],
        "silence_threshold_s": int(supervisor.get("silence_threshold_minutes") or 10) * 60,
    }


def cleanup_profile_locks(profile_dir: Path) -> None:
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        path = profile_dir / name
        try:
            if path.is_symlink() or path.exists():
                path.unlink()
                log.info("removed stale Chromium lock: %s", path.name)
        except FileNotFoundError:
            pass
        except Exception as exc:
            log.warning("could not remove %s: %s", name, exc)


async def do_login(page, login: str, password: str) -> bool:
    try:
        await page.fill('input[name="username"]', login, timeout=10000)
        await page.fill('input[type="password"]', password, timeout=10000)
        await page.click("#btnLogin", timeout=10000)
        await page.wait_for_url(lambda url: "/my/login" not in url, timeout=20000)
        return True
    except Exception as exc:
        log.error("login form submit failed: %s", exc)
        return False


async def ensure_logged_in(context, login: str, password: str) -> bool:
    page = await context.new_page()
    try:
        await page.goto(MODERATOR_CHECK_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        if "/my/login" not in page.url:
            log.info("already logged in")
            return True
        log.info("logging in on start.bizon365.ru...")
        ok = await do_login(page, login, password)
        if ok:
            log.info("login successful")
        else:
            log.error("login failed, still at: %s", page.url)
        return ok
    except Exception as exc:
        log.error("ensure_logged_in error: %s", exc)
        return False
    finally:
        await page.close()


async def send_telegram_alert(message: str) -> None:
    log.warning("alert: %s", message.replace("\n", " | ")[:500])


def _schedule_console_alert(title: str, room_name: str, text: str, throttle_s: int = 600) -> None:
    key = f"{title}:{room_name}:{text[:120]}"
    now = time.monotonic()
    if now - _console_alert_last.get(key, 0.0) < throttle_s:
        return
    _console_alert_last[key] = now
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_telegram_alert(f"{title}\nRoom: {room_name}\n{text[:900]}"))
    except RuntimeError:
        log.warning("cannot schedule alert: no running event loop")


def _on_console(name: str, msg) -> None:
    text = msg.text
    if msg.type == "error":
        log.warning("%s JS error: %s", name, text[:500])
        _schedule_console_alert("BIZON JS ERROR", name, text)
        return
    if "[MODERATOR]" in text or "[NEXUS-BIZON]" in text:
        log.info("%s JS: %s", name, text[:500])
        activity.touch()
        if "Message queued" in text:
            reply_health.mark_queued()
        if "Reply sent" in text:
            reply_health.mark_replied()
        if any(marker in text for marker in ("API error", "Reply failed", "Bot disabled")):
            _schedule_console_alert("BIZON MODERATOR FAILURE", name, text)


def _build_room_login_url(room_url: str) -> str:
    sec = room_url.split("sec=")[-1].split("&")[0]
    room_path = room_url.split("?")[0].replace("https://start.bizon365.ru", "")
    return f"{MODERATOR_LOGIN_URL}?redirect={room_path}%3Fsec%3D{sec}"


async def enter_room(page, room_url: str, name: str, login: str, password: str) -> bool:
    target = _build_room_login_url(room_url)
    await page.goto(target, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    if "/my/login" in page.url:
        if not await do_login(page, login, password):
            log.error("%s login failed at %s", name, page.url)
            return False
    if "sec=" not in page.url:
        log.warning("%s sec missing after redirect, forcing direct nav", name)
        await page.goto(room_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    return True


async def wait_for_chatframe(page, name: str) -> bool:
    try:
        await page.wait_for_selector("#chatframe, ul#chat, #chat_container", timeout=READY_TIMEOUT_MS)
        log.info("%s READY", name)
        return True
    except Exception as exc:
        log.error("%s chatframe not found: %s", name, exc)
        return False


async def heartbeat(page, name: str) -> bool:
    try:
        if page.is_closed():
            return False
        chatframe = await page.query_selector("#chatframe, ul#chat, #chat_container")
        if not chatframe:
            log.warning("%s heartbeat FAIL - chatframe gone", name)
            return False
        return True
    except Exception as exc:
        log.error("%s heartbeat error: %s", name, exc)
        return False


async def keep_room_alive(context, url: str, name: str, login: str, password: str, start_delay: float = 0) -> None:
    await asyncio.sleep(start_delay)
    while not _stop_requested:
        page = None
        try:
            page = await context.new_page()
            page.on("console", lambda msg, room_name=name: _on_console(room_name, msg))

            if not await enter_room(page, url, name, login, password):
                raise RuntimeError("could not enter room")
            if not await wait_for_chatframe(page, name):
                raise RuntimeError("chatframe not found")

            while not _stop_requested:
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                if not await heartbeat(page, name):
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("%s error: %s", name, exc)
        finally:
            if page and not page.is_closed():
                try:
                    await page.close()
                except Exception:
                    pass

        if not _stop_requested:
            log.info("%s reconnecting in %ss...", name, RECONNECT_DELAY_S)
            await asyncio.sleep(RECONNECT_DELAY_S)


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def in_silence_window(windows: list[dict], now: datetime | None = None) -> bool:
    now = now or datetime.now()
    cur = now.hour * 60 + now.minute
    for window in windows:
        try:
            sh, sm = _parse_hhmm(str(window["start"]))
            eh, em = _parse_hhmm(str(window["end"]))
        except Exception:
            continue
        if sh * 60 + sm <= cur < eh * 60 + em:
            return True
    return False


def _seconds_until(target_hhmm: str, now: datetime | None = None) -> float:
    now = now or datetime.now()
    hour, minute = _parse_hhmm(target_hhmm)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def scheduled_restart_task(restart_time: str, restart_event: asyncio.Event) -> None:
    while not _stop_requested:
        wait_s = _seconds_until(restart_time)
        next_at = datetime.now() + timedelta(seconds=wait_s)
        log.info("next scheduled restart at %s (in %s min)", next_at.strftime("%Y-%m-%d %H:%M"), int(wait_s / 60))
        await asyncio.sleep(wait_s)
        log.warning("=== SCHEDULED RESTART ===")
        restart_event.set()
        return


async def silence_watchdog_task(windows: list[dict], threshold_s: int) -> None:
    if not windows:
        log.info("silence watchdog disabled (no windows configured)")
        return
    log.info("silence watchdog: %s window(s), threshold=%ss", len(windows), threshold_s)
    alerted_key: str | None = None
    while not _stop_requested:
        await asyncio.sleep(60)
        now = datetime.now()
        if not in_silence_window(windows, now):
            alerted_key = None
            continue
        silent_s = activity.seconds_since()
        window_key = f"{now.date().isoformat()}_{now.hour // 12}"
        if silent_s > threshold_s and alerted_key != window_key:
            await send_telegram_alert(
                "BIZON SILENCE\n"
                f"No [MODERATOR] events for {int(silent_s / 60)} min\n"
                f"Time: {now.strftime('%Y-%m-%d %H:%M')}\n"
                "Check Bizon module log"
            )
            alerted_key = window_key


async def reply_watchdog_task(windows: list[dict], threshold_s: int = 300) -> None:
    if not windows:
        log.info("reply watchdog disabled (no windows configured)")
        return
    log.info("reply watchdog: threshold=%ss", threshold_s)
    while not _stop_requested:
        await asyncio.sleep(60)
        now = datetime.now()
        if not in_silence_window(windows, now):
            continue
        stalled_s = reply_health.seconds_since_last_reply_after_queue()
        if stalled_s is None or stalled_s < threshold_s:
            continue
        if time.monotonic() - reply_health.last_alert_at < 900:
            continue
        reply_health.last_alert_at = time.monotonic()
        await send_telegram_alert(
            "BIZON MODERATOR STALLED\n"
            f"Queued messages: {reply_health.queued}\n"
            f"Replies sent: {reply_health.replied}\n"
            f"No reply after latest queue for {int(stalled_s / 60)} min\n"
            f"Time: {now.strftime('%Y-%m-%d %H:%M')}"
        )


async def run_once(cfg: dict[str, Any]) -> int:
    from playwright.async_api import async_playwright

    profile_dir = Path(cfg["profile_dir"])
    profile_dir.mkdir(parents=True, exist_ok=True)
    cleanup_profile_locks(profile_dir)

    log.info(
        "starting headless browser, rooms=%s, restart=%s, silence_windows=%s",
        len(cfg["urls"]),
        cfg["scheduled_restart_time"],
        len(cfg["silence_windows"]),
    )
    restart_event = asyncio.Event()

    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--disable-infobars"],
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        if not await ensure_logged_in(context, cfg["login"], cfg["password"]):
            log.error("could not authenticate, exiting run")
            await context.close()
            return 1

        tasks: list[asyncio.Task] = []
        for i, url in enumerate(cfg["urls"]):
            name = url.split("/")[-1].split("?")[0]
            tasks.append(asyncio.create_task(
                keep_room_alive(context, url, name, cfg["login"], cfg["password"], start_delay=i * ROOM_OPEN_DELAY_S),
                name=f"room:{name}",
            ))
        tasks.append(asyncio.create_task(scheduled_restart_task(cfg["scheduled_restart_time"], restart_event), name="scheduled_restart"))
        tasks.append(asyncio.create_task(silence_watchdog_task(cfg["silence_windows"], cfg["silence_threshold_s"]), name="silence_watchdog"))
        tasks.append(asyncio.create_task(reply_watchdog_task(cfg["silence_windows"]), name="reply_watchdog"))

        try:
            while not _stop_requested and not restart_event.is_set():
                await asyncio.sleep(1)
        finally:
            log.info("shutting down tasks...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await context.close()
    return 0


async def run_loop(config_path: Path) -> None:
    global _runtime_cfg
    attempt = 0
    while not _stop_requested:
        attempt += 1
        try:
            cfg = load_config(config_path)
            _runtime_cfg = cfg
            setup_logging(Path(cfg["log_file"]))
            log.info("runner attempt #%s", attempt)
            exit_code = await run_once(cfg)
        except Exception as exc:
            setup_logging(config_path.parent / "logs" / "module.log")
            log.error("runner attempt failed: %s", exc, exc_info=True)
            exit_code = 1
        if _stop_requested:
            break
        delay = 5 if exit_code == 0 else 10
        log.info("run exited with code %s, restarting in %ss", exit_code, delay)
        await asyncio.sleep(delay)
    log.info("runner stopped")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: runner.py /path/to/runner_config.json", file=sys.stderr)
        sys.exit(2)
    config_path = Path(sys.argv[1]).resolve()
    if not acquire_instance_lock(config_path):
        sys.exit(3)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    asyncio.run(run_loop(config_path))


if __name__ == "__main__":
    main()
