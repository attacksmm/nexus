import asyncio
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace

from fastapi.responses import JSONResponse
from starlette.requests import Request

from module_tilda_chat_links import router as module


def _request(path: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": path, "headers": []})


def _create_runs_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                platform TEXT,
                title TEXT,
                stream_number TEXT,
                course_key TEXT,
                test_mode INTEGER,
                status TEXT,
                link TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO runs VALUES(?,?,?,?,?,?,?,?)",
            [
                (1, "telegram", "57 TG", "57", "puppy", 0, "ok", "https://t.me/57"),
                (2, "vk", "57 VK", "57", "puppy", 0, "needs_members", "https://vk.me/join/57"),
                (3, "vk", "58 VK", "58", "puppy", 0, "ok", "https://vk.me/join/58"),
                (4, "telegram", "99 TG", "99", "puppy", 1, "ok", "https://t.me/test"),
            ],
        )


def test_course_creator_catalog_publishes_only_complete_production_pairs() -> None:
    with tempfile.TemporaryDirectory() as directory:
        module_dir = Path(directory) / "modules" / "tilda-chat-links"
        db_path = module_dir.parent / "course-chat-creator" / "data" / "course-chat-creator.db"
        db_path.parent.mkdir(parents=True)
        _create_runs_db(db_path)
        previous_ctx = module._ctx
        module._ctx = SimpleNamespace(module_dir=module_dir)
        try:
            catalog = module._course_chat_creator_catalog("puppy")
        finally:
            module._ctx = previous_ctx

    assert [item["number"] for item in catalog["tg"]] == [57]
    assert [item["number"] for item in catalog["vk"]] == [57]
    assert catalog["vk"][0]["source"] == "course_chat_creator"


def test_latest_pair_never_mixes_streams() -> None:
    catalog = {
        "tg": [
            {"number": 56, "url": "https://t.me/56"},
            {"number": 57, "url": "https://t.me/57"},
        ],
        "vk": [
            {"number": 56, "url": "https://vk.me/join/56"},
            {"number": 58, "url": "https://vk.me/join/58"},
        ],
    }

    pair = module._latest_pair_from_catalog(catalog)

    assert pair["stream_number"] == 56
    assert pair["tg"]["url"].endswith("/56")
    assert pair["vk"]["url"].endswith("/56")


def test_sheet_pair_overrides_same_stream_from_course_creator() -> None:
    created = {
        "title": "Курс",
        "tg": [{"number": 57, "url": "https://t.me/generated"}],
        "vk": [{"number": 57, "url": "https://vk.me/join/generated"}],
    }
    sheet = {
        "tg": [{"number": 57, "url": "https://t.me/manual", "source": "sheet"}],
        "vk": [{"number": 57, "url": "https://vk.me/join/manual-250", "source": "sheet"}],
    }

    merged = module._merge_chat_catalog(created, sheet)

    assert merged["tg"][0]["url"] == "https://t.me/manual"
    assert merged["vk"][0]["url"] == "https://vk.me/join/manual-250"


def test_loaded_catalog_does_not_publish_course_creator_links(monkeypatch) -> None:
    async def fetch_csv(gid: str) -> str:
        return (
            '"Щенок 57","https://vk.me/join/manual-250"\n'
            if gid == module.CHAT_SHEETS["puppy"]["vk"]
            else '"Щенок 57","https://t.me/manual"\n'
        )

    monkeypatch.setattr(module, "_fetch_csv", fetch_csv)
    monkeypatch.setattr(
        module,
        "_course_chat_creator_catalog",
        lambda _course: {
            "vk": [{"number": 57, "url": "https://vk.me/join/generated"}],
            "tg": [{"number": 57, "url": "https://t.me/generated"}],
        },
    )
    module._cache.clear()

    catalog = asyncio.run(module._load_chat_catalog("puppy"))

    assert catalog["vk"][0]["url"] == "https://vk.me/join/manual-250"
    assert catalog["tg"][0]["url"] == "https://t.me/manual"


def test_private_sheet_quota_error_falls_back_to_public_csv(monkeypatch, tmp_path) -> None:
    credentials = tmp_path / "service-account.json"
    credentials.write_text("{}", encoding="utf-8")

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'"Puppy 57","https://vk.me/join/manual-250"\n'

    monkeypatch.setattr(module, "_google_credentials_path", lambda: credentials)
    monkeypatch.setattr(
        module,
        "_fetch_csv_private_sync",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("429 Too Many Requests")),
    )
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    result = asyncio.run(module._fetch_csv("65520414"))

    assert "manual-250" in result


def test_status_requires_panel_access(monkeypatch) -> None:
    async def forbidden(_request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    monkeypatch.setattr(module, "_require_panel_user", forbidden)

    response = asyncio.run(module.status(_request("/status")))

    assert response.status_code == 403


def test_debug_chats_requires_panel_access(monkeypatch) -> None:
    async def forbidden(_request):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    async def must_not_load():
        raise AssertionError("chat catalog must stay private")

    monkeypatch.setattr(module, "_require_panel_user", forbidden)
    monkeypatch.setattr(module, "_load_current_chats", must_not_load)

    response = asyncio.run(module.debug_chats(_request("/debug/chats")))

    assert response.status_code == 403
