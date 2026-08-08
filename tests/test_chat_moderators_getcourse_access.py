from module_chat_moderators import router as module


def test_access_change_adds_course_bridge_without_package(monkeypatch):
    catalog = [
        {"group_id": 4059685, "name": "Знакомство. Щенок", "course_key": "puppy", "group_kind": "root", "managed": True},
        {"group_id": 4306384, "name": "Выдача Щенка без процесса", "course_key": "puppy", "group_kind": "bridge", "managed": True},
        {"group_id": 4059687, "name": "1 модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": 1, "managed": True},
    ]
    captured = {}
    monkeypatch.setattr(module.NexusGetCourseAccessService, "_catalog", lambda self: catalog)
    monkeypatch.setattr(module, "_gc_create_group_backup", lambda **kwargs: 7)
    monkeypatch.setattr(module, "_gc_create_access_request", lambda **kwargs: captured.update(kwargs))

    result = module.service_prepare_access_change(
        gc_user_id="100",
        email="student@example.com",
        current_groups=[{"group_id": "999", "name": "Служебная группа"}],
        changes=[{"group_id": "4059687", "enabled": True}],
        requester_user_id="1",
    )

    names = {item["name"] for item in captured["target_groups"]}
    assert result["ok"] is True
    assert names == {"Служебная группа", "Выдача Щенка без процесса", "1 модуль. Щенок"}


def test_pending_request_cannot_add_intro_group(monkeypatch):
    service = module.NexusGetCourseAccessService()
    request = {
        "status": "pending",
        "command_text": "streams-access",
        "current_groups": [{"name": "Премиум. Собака"}],
        "target_groups": [{"name": "Премиум. Собака"}, {"name": "Тест-драйв. Собака"}],
    }
    statuses = []
    monkeypatch.setattr(module, "_gc_get_access_request", lambda _request_id: request)
    monkeypatch.setattr(service, "_catalog", lambda: [
        {"name": "Тест-драйв. Собака", "managed": True, "group_kind": "root"},
    ])
    monkeypatch.setattr(module, "_gc_mark_request_status", lambda *args, **kwargs: statuses.append(kwargs["status"]))

    try:
        service.apply_access_request("old-preview")
        assert False, "obsolete preview must be rejected"
    except module.GetCourseAccessError as error:
        assert str(error) == "Проверка устарела. Выберите доступы заново"
    assert statuses == ["cancelled"]


def test_access_change_with_package_adds_only_module(monkeypatch):
    catalog = [
        {"group_id": 4059685, "name": "Знакомство. Щенок", "course_key": "puppy", "group_kind": "root", "managed": True},
        {"group_id": 4306384, "name": "Выдача Щенка без процесса", "course_key": "puppy", "group_kind": "bridge", "managed": True},
        {"group_id": 4059658, "name": "Премиум. Щенок", "course_key": "puppy", "group_kind": "package", "package_key": "premium", "managed": True},
        {"group_id": 4059688, "name": "2 модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": 2, "managed": True},
    ]
    captured = {}
    monkeypatch.setattr(module.NexusGetCourseAccessService, "_catalog", lambda self: catalog)
    monkeypatch.setattr(module, "_gc_create_group_backup", lambda **kwargs: 8)
    monkeypatch.setattr(module, "_gc_create_access_request", lambda **kwargs: captured.update(kwargs))

    result = module.service_prepare_access_change(
        gc_user_id="100",
        email="student@example.com",
        current_groups=[
            {"group_id": "4059685", "name": "Знакомство. Щенок"},
            {"group_id": "4059658", "name": "Премиум. Щенок"},
        ],
        changes=[{"group_id": "4059688", "enabled": True}],
        requester_user_id="1",
    )

    assert result["added"] == ["2 модуль. Щенок"]
    assert {item["name"] for item in captured["target_groups"]} == {
        "Знакомство. Щенок",
        "Премиум. Щенок",
        "2 модуль. Щенок",
    }
