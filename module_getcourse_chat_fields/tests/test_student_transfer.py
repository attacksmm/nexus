import asyncio
import io
import json
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from module_getcourse_chat_fields import router as module


def test_busy_access_export_is_deferred_for_automatic_retry(tmp_path, monkeypatch):
    module._db_path = tmp_path / "module.db"
    asyncio.run(module._init_db())
    monkeypatch.setattr(module, "_customer_db_path", lambda: tmp_path / "missing.db")

    async def catalog(_settings):
        return [{"group_id": "4059685", "name": "Знакомство. Щенок"}]

    async def rows(*_args, **_kwargs):
        return [], "Уже запущен один экспорт, попробуйте позднее"

    monkeypatch.setattr(module, "_live_access_group_catalog", catalog)
    monkeypatch.setattr(module, "_getcourse_export_rows", rows)
    result = asyncio.run(module.service_getcourse_access_snapshot(email="student@example.com", live=True, force=True))
    assert result["ok"] is False
    assert result["error"] == "Обновление отложено"
    assert result["refresh_due"] is True
    assert result["next_at"]


def test_curator_display_name_uses_existing_mapping():
    settings = {"curator_map": "Ирина=Куратор 1;Слава=Куратор 2;Настасья=Куратор 3"}
    assert module._transfer_curator_raw(settings, "Куратор 1") == "Ирина"
    assert module._transfer_curator_raw(settings, "Куратор 2") == "Слава"
    assert module._transfer_curator_raw(settings, "Куратор 3") == "Настасья"


def test_transfer_move_uses_cached_flow_snapshot_once(tmp_path, monkeypatch):
    credentials = tmp_path / "google.json"
    credentials.write_text("{}", encoding="utf-8")
    calls = []
    thread_calls = []
    snapshot = {"ok": True, "items": [
        {
            "course_key": "puppy", "course": "Щенок", "stream": "57", "sheet_id": 1,
            "sheet_title": "Щ57 (31.07)", "students": [{"email": "ivan@example.com", "row": 8}],
        },
        {
            "course_key": "dog", "course": "Собака", "stream": "54", "sheet_id": 2,
            "sheet_title": "С54 (22.07)", "curator_value": "Куратор 3", "students": [],
        },
    ]}

    async def settings():
        return {}

    async def flow_students(_settings, refresh=False):
        calls.append(refresh)
        return snapshot

    async def to_thread(_func, **_kwargs):
        thread_calls.append(_kwargs)
        return {"ok": True, "status": "moved", "target_row": 12}

    monkeypatch.setattr(module, "_settings_map", settings)
    monkeypatch.setattr(module, "_flow_students", flow_students)
    monkeypatch.setattr(module, "_curator_spreadsheet_id", lambda _settings: "sheet")
    monkeypatch.setattr(module, "_curator_credentials_path", lambda _settings: credentials)
    monkeypatch.setattr(module.asyncio, "to_thread", to_thread)
    result = asyncio.run(module.service_transfer_move_student(
        email="ivan@example.com", source_course_key="puppy", source_stream="57", source_row=8,
        target_course_key="dog", target_stream="54", move=False,
    ))
    assert calls == [False]
    assert thread_calls[0]["move"] is False
    assert result["student"]["row"] == 12


def test_transfer_move_removes_source_when_target_already_exists(tmp_path, monkeypatch):
    from google.auth.transport import requests as google_requests
    from google.oauth2 import service_account

    class Response:
        def __init__(self, payload=None):
            self.payload = payload or {}

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Session:
        def __init__(self):
            self.posts = []

        def get(self, *_args, **_kwargs):
            return Response({"valueRanges": [
                {"values": [[], [], [], [], [], [],
                    ["ФИО", "Дата", "Курс", "Тариф", "Оформлен", "TG аккаунт", "Почта"],
                    ["Иван", 45886, "Щенок", "Премиум", "Геткурс", "@ivan", "ivan@example.com"]]},
                {"values": [
                    ["ФИО", "Дата", "Курс", "Тариф", "менеджер", "ответственный куратор", "TG/VK аккаунт", "Почта"],
                    ["Иван", "", "Собака", "Премиум", "", "", "", "ivan@example.com"],
                ]},
            ]})

        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            return Response()

    session = Session()
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(google_requests, "AuthorizedSession", lambda _credentials: session)

    result = module._transfer_sheet_move_sync(
        spreadsheet_id="sheet", credentials_path=tmp_path / "google.json",
        source_sheet_id=1, source_sheet_title="Щ57 (31.07)", source_row=8,
        target_sheet_id=2, target_sheet_title="С54 (22.07)", students_range="A1:AC300",
        email="ivan@example.com", target_course="Собака", target_curator="Настасья",
    )

    assert result == {
        "ok": True, "status": "duplicate_removed", "target_row": 2, "source_row_deleted": True,
    }
    assert session.posts[0][0].endswith(":batchUpdate")
    assert session.posts[0][1]["json"] == {"requests": [{"deleteDimension": {"range": {
        "sheetId": 1, "dimension": "ROWS", "startIndex": 7, "endIndex": 8,
    }}}]}


def test_transfer_copy_keeps_source_row(tmp_path, monkeypatch):
    from google.auth.transport import requests as google_requests
    from google.oauth2 import service_account

    class Response:
        def __init__(self, payload=None):
            self.payload = payload or {}

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Session:
        def __init__(self):
            self.posts = []
            self.gets = 0

        def get(self, *_args, **_kwargs):
            self.gets += 1
            if self.gets == 1:
                return Response({"valueRanges": [
                    {"values": [[], [], [], [], [], [],
                        ["ФИО", "Дата", "Курс", "Тариф", "Оформлен", "TG аккаунт", "Почта", "Доб. в купивших", "Чат", "0.0", "ВИП1", "1.0"],
                        ["Иван", 45886, "Щенок + Собака", "Премиум", "Геткурс", "@ivan", "ivan@example.com", False, True, False, False, True]]},
                    {"values": [
                        ["ФИО", "Дата", "Тариф", "Оформлен", "менеджер", "ответственный куратор", "TG/VK аккаунт", "Почта", "Доб. в купивших", "Чат", "0.0", "ВИП1", "1.0"],
                        ["Шаблон", 46242, "Премиум", "Геткурс", "Татьяна", "", "@template", "template@example.com", False, True, False, False, False],
                        ["", "", "", "", "", "", "", "", False, False, False, False, False],
                    ]},
                ]})
            return Response({"values": [["Иван", 45886, "Премиум", "Геткурс", "Татьяна", "", "@ivan", "ivan@example.com"]]})

        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            return Response()

    session = Session()
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(google_requests, "AuthorizedSession", lambda _credentials: session)

    result = module._transfer_sheet_move_sync(
        spreadsheet_id="sheet", credentials_path=tmp_path / "google.json",
        source_sheet_id=1, source_sheet_title="Щ57 (31.07)", source_row=8,
        target_sheet_id=2, target_sheet_title="С54 (22.07)", students_range="A1:AC300",
        email="ivan@example.com", target_course="Собака", target_curator="Настасья",
        student={"manager_name": "Татьяна", "tg_account": "@ivan"}, move=False,
    )

    assert result["status"] == "copied"
    assert result["target_row"] == 3
    assert result["source_row_deleted"] is False
    requests = session.posts[0][1]["json"]["requests"]
    assert {item["copyPaste"]["pasteType"] for item in requests} == {"PASTE_FORMAT", "PASTE_DATA_VALIDATION"}
    assert requests[0]["copyPaste"]["destination"] == {
        "sheetId": 2, "startRowIndex": 2, "endRowIndex": 3,
        "startColumnIndex": 0, "endColumnIndex": 29,
    }
    writes = {item["range"]: item["values"][0][0] for item in session.posts[1][1]["json"]["data"]}
    assert writes["'С54 (22.07)'!E3"] == "Татьяна"
    assert writes["'С54 (22.07)'!G3"] == "@ivan"
    assert writes["'С54 (22.07)'!H3"] == "ivan@example.com"
    assert writes["'С54 (22.07)'!J3"] is True
    assert writes["'С54 (22.07)'!M3"] is True
    assert "'С54 (22.07)'!F3" not in writes


def test_order_identity_lookup_returns_phone_and_tariff(tmp_path, monkeypatch):
    db_path = tmp_path / "customer-db.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE cdb_getcourse_orders (
                id INTEGER PRIMARY KEY,
                platform_id TEXT NOT NULL,
                custom_fields TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        db.execute(
            "INSERT INTO cdb_getcourse_orders VALUES(?,?,?,?,?)",
            (
                91,
                "873857315",
                json.dumps({
                    "order_id": "873857315",
                    "gc_user_id": "511441775",
                    "email": "student@example.com",
                    "phone": "+7 999 111-22-33",
                    "utm_term": "platform_id=268030521",
                    "title": "Курс Щенок тариф Премиум",
                    "status": "Завершён",
                    "payment_state": "paid",
                    "chat_fields_course_key": "puppy",
                    "Поток": "57",
                    "Ссылка на чат ВК": "https://vk/57",
                    "Ссылка на чат ТГ": "https://t.me/57",
                    "Номер куратора": "Куратор 3",
                }),
                "2026-08-01T10:00:00Z",
                "2026-08-01T10:00:00Z",
            ),
        )
        db.execute(
            "INSERT INTO cdb_getcourse_orders VALUES(?,?,?,?,?)",
            (
                92,
                "newer-order",
                json.dumps({"order_id": "newer-order", "gc_user_id": "other", "email": "student@example.com"}),
                "2026-08-02T10:00:00Z",
                "2026-08-02T10:00:00Z",
            ),
        )
    monkeypatch.setattr(module, "_customer_db_path", lambda: db_path)
    result = asyncio.run(module.service_order_identities(identities=[{
        "key": "student-1",
        "source_record_id": 91,
        "gc_user_id": "511441775",
        "email": "student@example.com",
    }]))
    assert result == {
        "ok": True,
        "items": [{
            "key": "student-1", "phone": "+7 999 111-22-33", "tariff": "Premium",
            "utm_term": "platform_id=268030521", "product_kind": "single",
            "assignment": {
                "course_key": "puppy", "stream": "57", "vk_link": "https://vk/57",
                "tg_link": "https://t.me/57", "curator": "Куратор 3",
            },
        }],
    }


def test_cached_access_reads_exact_group_ids(tmp_path, monkeypatch):
    db_path = tmp_path / "customer-db.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            "CREATE TABLE cdb_getcourse_users(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)"
        )
        db.execute(
            "INSERT INTO cdb_getcourse_users VALUES(1,?,?,?,?)",
            (
                "511441775",
                json.dumps({"email": "student@example.com", "getcourse_group_membership": "4059686:2026-01-01, 4059687:2026-01-08"}),
                "2026-07-18T00:00:00Z",
                "2026-07-18T00:00:00Z",
            ),
        )
    monkeypatch.setattr(module, "_customer_db_path", lambda: db_path)
    module._db_path = tmp_path / "module.db"
    asyncio.run(module._init_db())
    result = asyncio.run(module.service_getcourse_access_snapshot(gc_user_id="511441775"))
    assert result["ok"] is True
    assert result["groups"] == [
        {"group_id": "4059686", "added_at": "2026-01-01"},
        {"group_id": "4059687", "added_at": "2026-01-08"},
    ]


def test_live_access_is_saved_and_reused_for_one_hour(tmp_path, monkeypatch):
    module._db_path = tmp_path / "module.db"
    asyncio.run(module._init_db())
    monkeypatch.setattr(module, "_customer_db_path", lambda: tmp_path / "missing-customer.db")

    async def catalog(_settings):
        return [
            {"group_id": "4059685", "name": "Знакомство. Щенок"},
            {"group_id": "4059705", "name": "9 модуль (бонусный). Щенок"},
        ]

    calls = []

    async def rows(_path, _params, _settings, _purpose):
        calls.append(1)
        return [
            {
                "id": "511441775",
                "email": "student@example.com",
                "idgrouplist": "4059685:2026-01-01,4059705:2026-02-01",
            }
        ], ""

    monkeypatch.setattr(module, "_live_access_group_catalog", catalog)
    monkeypatch.setattr(module, "_getcourse_export_rows", rows)
    first = asyncio.run(
        module.service_getcourse_access_snapshot(
            gc_user_id="511441775", email="student@example.com", live=True, force=True
        )
    )
    second = asyncio.run(
        module.service_getcourse_access_snapshot(gc_user_id="511441775", email="student@example.com", live=True)
    )
    assert [item["group_id"] for item in first["groups"]] == ["4059685", "4059705"]
    assert second["source"] == "cache"
    assert len(calls) == 1


def test_batch_access_sync_saves_enrolled_users(tmp_path, monkeypatch):
    module._db_path = tmp_path / "module.db"
    asyncio.run(module._init_db())

    async def rows(path, params, _settings, _purpose):
        assert path == "/pl/api/account/groups/4059685/users"
        assert params["added_at[from]"] == "2000-01-01"
        assert params["idgrouplist"] == "id_date"
        return [
            {
                "id": "511441775",
                "email": "student@example.com",
                "idgrouplist": "4059685:2026-01-01,4059705:2026-02-01",
            }
        ], ""

    monkeypatch.setattr(module, "_getcourse_export_rows", rows)
    catalog = [
        {"group_id": "4059685", "name": "Знакомство. Щенок"},
        {"group_id": "4059705", "name": "9 модуль (бонусный). Щенок"},
    ]
    result = asyncio.run(
        module.service_sync_getcourse_access_snapshots(
            identities=[{"gc_user_id": "511441775", "email": "student@example.com"}],
            catalog=catalog,
            root_group_ids=["4059685"],
        )
    )
    cached = asyncio.run(
        module.service_getcourse_access_snapshot(gc_user_id="511441775", email="student@example.com")
    )
    assert result["updated"] == 1
    assert cached["groups"][1]["group_id"] == "4059705"


def test_registry_reads_are_chunked_below_google_url_limit():
    calls = []

    class Response:
        def __init__(self, count):
            self.count = count

        def raise_for_status(self):
            return None

        def json(self):
            return {"valueRanges": [{"values": [[index]]} for index in range(self.count)]}

    class Session:
        def get(self, url, params, timeout):
            count = sum(key == "ranges" for key, _value in params)
            calls.append(count)
            return Response(count)

    rows = module._registry_batch_rows(Session(), "sheet", [f"Поток {index}" for index in range(32)])
    assert calls == [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2]
    assert len(rows) == 32


def test_shutdown_cancels_background_workers():
    async def check():
        tasks = [asyncio.create_task(asyncio.sleep(10)) for _ in range(3)]
        module._poll_task, module._gc_lookup_task, module._gc_write_task = tasks
        await module.shutdown()
        assert all(task.cancelled() for task in tasks)
        assert module._poll_task is module._gc_lookup_task is module._gc_write_task is None

    asyncio.run(check())


def test_export_budget_does_not_count_import_updates(tmp_path):
    module._db_path = tmp_path / "module.db"
    asyncio.run(module._init_db())
    with sqlite3.connect(module._db_path) as db:
        db.executemany(
            "INSERT INTO gc_export_api_calls(purpose) VALUES(?)",
            [("students-fields:user",), ("students-fields:deal",), ("access-user:start",)],
        )
    assert asyncio.run(module._gc_export_calls_used()) == 1


def test_registry_xlsx_fallback_reads_sparse_strings_and_checkboxes():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "xl/workbook.xml",
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Щ57 " sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/sharedStrings.xml",
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>Email</t></si><si><t>Урок 1</t></si><si><t>student@example.com</t></si></sst>',
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="G1" t="s"><v>0</v></c><c r="H1" t="s"><v>1</v></c></row><row r="2"><c r="G2" t="s"><v>2</v></c><c r="H2" t="b"><v>1</v></c></row></sheetData></worksheet>',
        )
    rows = module._registry_xlsx_rows(output.getvalue())["Щ57 "]
    assert rows[0][6:8] == ["Email", "Урок 1"]
    assert rows[1][6:8] == ["student@example.com", True]


def test_flow_catalog_uses_stale_registry_when_google_links_are_throttled(monkeypatch):
    async def settings():
        return {}

    async def throttled(_settings):
        return {"ok": False, "items": [], "errors": [{"error": "429"}]}

    async def cached(*_args, **_kwargs):
        return {"items": [{"course_key": "puppy", "stream": "57", "vk_link": "https://vk.example", "tg_link": "https://t.me/example"}]}

    monkeypatch.setattr(module, "_settings_map", settings)
    monkeypatch.setattr(module, "_chat_flows", throttled)
    monkeypatch.setattr(module, "_flow_students_cache_key", lambda _settings: "cache")
    monkeypatch.setattr(module, "_load_flow_students_cache", cached)
    result = asyncio.run(module.service_flow_catalog())
    assert result["ok"] is True
    assert result["stale"] is True
    assert result["items"][0]["stream"] == "57"


def test_flow_dates_follow_sheet_titles_and_payment_date():
    flows = [
        {"course_key": "puppy", "stream": "57", "curator_sheet": "Щ57 (31.07)", "vk_link": "https://vk/57", "tg_link": "https://t.me/57"},
        {"course_key": "puppy", "stream": "35", "curator_sheet": "Щ35 (03.01)", "vk_link": "https://vk/35", "tg_link": "https://t.me/35"},
        {"course_key": "puppy", "stream": "34", "curator_sheet": "Щ34 (20.12)", "vk_link": "https://vk/34", "tg_link": "https://t.me/34"},
    ]
    module._add_flow_start_dates(flows, datetime(2026, 8, 3, tzinfo=timezone.utc))
    assert [item["date_start"] for item in flows] == ["2026-07-31", "2026-01-03", "2025-12-20"]
    assert module._dated_flow_for_order({"items": flows}, "puppy", {"paid_at": "2026-08-02T10:00:00Z"})["stream"] == "57"
    assert module._dated_flow_for_order({"items": flows}, "puppy", {"paid_at": "2025-09-04T10:00:00Z"}) is None


def test_business_date_prefers_real_payment_and_blank_checkboxes_are_false():
    fields = {"paid_at": "2025-09-04T14:31:34Z", "received_at": "2026-07-18T20:31:47Z"}
    assert module._business_order_date_text(fields) == "2025-09-04T14:31:34Z"
    assert module._registry_lesson_values(["student@example.com"], [{"key": "H", "column": 7}]) == {"H": False}
    assert module._registry_lesson_values(["FALSE"], [{"key": "H", "column": 0}]) == {"H": False}
    assert module._registry_lesson_values(["TRUE"], [{"key": "H", "column": 0}]) == {"H": True}


def test_registry_row_schema_keeps_manager_and_curator_in_their_columns():
    rows = [
        [], [], [], [], [], [],
        ["ФИО", "Дата", "Курс", "Тариф", "Менеджер", "Ответственный куратор", "TG аккаунт", "Почта", "Чат", "ВИП1", "1.0"],
        ["Иван", "03.08.2026", "Щенок", "Премиум", "Татьяна", "Настасья", "@ivan", "ivan@example.com", True, False, True],
    ]
    header, columns = module._sheet_student_header(rows)
    lessons = module._registry_lesson_columns(rows, header)
    assert columns == {
        "name": 0, "date": 1, "course": 2, "tariff": 3, "manager": 4,
        "responsible_curator": 5, "tg_account": 6, "email": 7,
    }
    assert [(item["key"], item["label"]) for item in lessons] == [("I", "Чат"), ("J", "ВИП1"), ("K", "1.0")]
    assert module._registry_date("2026-08-03T10:00:00Z") == "03.08.2026"
    assert module._registry_tariff("Premium") == "Премиум"


def test_entitled_orders_follow_updates_not_only_new_ids(tmp_path, monkeypatch):
    db_path = tmp_path / "customer.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            "CREATE TABLE cdb_getcourse_orders(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)"
        )
        db.executemany(
            "INSERT INTO cdb_getcourse_orders VALUES(?,?,?,?,?)",
            [
                (1, "old-updated", json.dumps({"email": "updated@example.com"}), "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z"),
                (2, "newer-id", json.dumps({"email": "earlier@example.com"}), "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"),
            ],
        )
    monkeypatch.setattr(module, "_customer_db_path", lambda: db_path)
    monkeypatch.setattr(module, "_chat_entitlement", lambda fields: {"eligible": True, "course_key": "puppy", "tariff": "standard"})

    result = asyncio.run(
        module.service_entitled_orders(after_source_record_id=2, after_updated_at="2026-01-02T00:00:00Z")
    )
    assert [item["source_record_id"] for item in result["items"]] == [1]
    assert result["cursor_updated_at"] == "2026-01-03T00:00:00Z"
    assert result["max_source_record_id"] == 2
    assert result["max_updated_id"] == 1


def test_curator_sheet_update_targets_exact_student_row(monkeypatch):
    from google.auth.transport import requests as google_requests
    from google.oauth2 import service_account

    captured = {}

    class Response:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self.data

    class Session:
        def __init__(self, credentials):
            pass

        def get(self, *args, **kwargs):
            return Response({"values": [
                ["ФИО", "Дата", "Курс", "Тариф", "Ответственный куратор", "TG аккаунт", "Email"],
                ["Иван", "", "Щенок", "", "Ирина", "", "ivan@example.com"],
            ]})

        def post(self, url, json, timeout):
            captured.update(url=url, body=json, timeout=timeout)
            return Response({})

    monkeypatch.setattr(google_requests, "AuthorizedSession", Session)
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", lambda *args, **kwargs: object())
    result = module._transfer_sheet_curator_sync(
        spreadsheet_id="sheet",
        credentials_path=Path("credentials.json"),
        sheet_id=77,
        sheet_title="Щ55",
        students_range="A1:AC300",
        source_row=2,
        email="ivan@example.com",
        curator_raw="Слава",
    )
    update = captured["body"]["requests"][0]["updateCells"]
    assert result["status"] == "updated"
    assert update["range"] == {
        "sheetId": 77,
        "startRowIndex": 1,
        "endRowIndex": 2,
        "startColumnIndex": 4,
        "endColumnIndex": 5,
    }
    assert update["rows"][0]["values"][0]["userEnteredValue"]["stringValue"] == "Слава"


def test_new_registry_row_clears_old_progress(monkeypatch):
    from google.auth.transport import requests as google_requests
    from google.oauth2 import service_account

    rows = [
        [], [], [], [], [], [],
        ["ФИО", "Дата", "Курс", "Тариф", "Менеджер", "Ответственный куратор", "TG аккаунт", "Почта", "Чат", "ВИП1", "1.0"],
        ["Шаблон", "", "Щенок", "Премиум", "", "", "", "template@example.com", True, True, True],
    ]
    value_writes = []

    class Response:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self.data

    class Session:
        def __init__(self, _credentials):
            self.values_reads = 0

        def get(self, url, **_kwargs):
            if "/values/" not in url:
                return Response({"sheets": [{"properties": {"sheetId": 57, "title": "Щ57 (31.07)"}}]})
            self.values_reads += 1
            if self.values_reads == 1:
                return Response({"values": rows})
            return Response({"values": [["Ученик", "03.08.2026", "Щенок", "Премиум", "Менеджер", "Настасья", "", "student@example.com", False, False, False]]})

        def post(self, url, json, **_kwargs):
            if url.endswith("/values:batchUpdate"):
                value_writes.extend(json["data"])
            return Response({})

    monkeypatch.setattr(google_requests, "AuthorizedSession", Session)
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", lambda *args, **kwargs: object())
    result = module._registry_ensure_student_sync(
        spreadsheet_id="sheet",
        credentials_path=Path("credentials.json"),
        course_key="puppy",
        stream="57",
        student={
            "name": "Ученик", "email": "student@example.com", "date": "2026-08-03T10:00:00Z",
            "course": "Щенок", "tariff": "Premium", "manager_name": "Менеджер", "curator_name": "Настасья",
        },
    )
    cleared = [item for item in value_writes if item["values"] == [[False]]]
    assert result["row"] == 9
    assert {item["range"] for item in cleared} == {
        "'Щ57 (31.07)'!I9", "'Щ57 (31.07)'!J9", "'Щ57 (31.07)'!K9",
    }


def test_tariff_format_plan_replaces_only_tariff_rules_for_latest_streams():
    tariff_rule = {
        "booleanRule": {
            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "Премиум"}]},
            "format": {"backgroundColor": {"red": 1, "green": 0.9, "blue": 0.7}},
        }
    }
    unrelated_rule = {
        "booleanRule": {
            "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Ошибка"}]},
            "format": {"textFormat": {"bold": True}},
        }
    }

    def sheet(sheet_id, title):
        return {
            "properties": {
                "sheetId": sheet_id, "title": title,
                "gridProperties": {"rowCount": 80, "columnCount": 32},
            },
            "conditionalFormats": [unrelated_rule, tariff_rule],
        }

    sheets = [sheet(51, "С51"), sheet(52, "С52"), sheet(56, "Щ56"), sheet(57, "Щ57")]
    rows = {
        "С52": [["ФИО", "Дата", "Курс", "Тариф", "Почта"]],
        "Щ57": [["ФИО", "Дата", "Курс", "Тариф", "Почта"]],
    }
    plan = module._registry_tariff_format_plan(sheets, rows, 1)

    assert [item["sheet_title"] for item in plan["items"]] == ["С52", "Щ57"]
    assert all(item["removed_rules"] == 1 for item in plan["items"])
    deletes = [item for item in plan["requests"] if "deleteConditionalFormatRule" in item]
    additions = [item for item in plan["requests"] if "addConditionalFormatRule" in item]
    assert [item["deleteConditionalFormatRule"]["index"] for item in deletes] == [1, 1]
    clears = [item for item in plan["requests"] if "repeatCell" in item]
    assert len(clears) == 2
    assert len(additions) == 8
    formulas = [
        item["addConditionalFormatRule"]["rule"]["booleanRule"]["condition"]["values"][0]["userEnteredValue"]
        for item in additions[:4]
    ]
    assert formulas == [
        '=$C2="Щенок+Собака"', '=$C2="Щенок + Собака"', '=$D2="ВИП"', '=$D2="Стандарт"',
    ]
    assert not module._is_tariff_conditional_rule(unrelated_rule)


def test_registry_manager_removes_technical_auto_suffix():
    assert module._registry_manager("Татьяна Воробьева (Авто)") == "Татьяна Воробьева"
    assert module._registry_manager("Татьяна Воробьева(Auto)") == "Татьяна Воробьева"
