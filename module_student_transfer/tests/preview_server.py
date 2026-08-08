import sys
import types
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from module_student_transfer import router as module


STUDENT = {
    "enrollment_id": "preview-ivan",
    "row": 8,
    "name": "Иван Петров",
    "email": "ivan@example.com",
    "gc_user_id": "100500",
    "order_id": "200600",
    "deal_number": "D-200600",
    "user_url": "https://club.sobakovod.pro/user/100500",
    "order_url": "https://club.sobakovod.pro/order/200600",
    "tariff": "Премиум",
    "responsible_curator": "Куратор 3",
    "lessons": [
        {"key": "I", "label": "Доб. в купивших", "value": True},
        {"key": "J", "label": "Чат", "value": True},
        {"key": "K", "label": "3", "value": False},
        {"key": "L", "label": "ВИП1", "value": True},
        {"key": "M", "label": "1.0", "value": True},
        {"key": "N", "label": "ВИП2", "value": True},
        {"key": "O", "label": "2.0", "value": True},
        {"key": "P", "label": "ВИП3", "value": True},
        {"key": "Q", "label": "3.0", "value": False},
        {"key": "R", "label": "ВИП4", "value": False},
        {"key": "S", "label": "4.0", "value": False},
        {"key": "T", "label": "ВИП5", "value": False},
        {"key": "U", "label": "5.0", "value": False},
        {"key": "V", "label": "ВИП6", "value": False},
        {"key": "W", "label": "6.0", "value": False},
        {"key": "X", "label": "ВИП7", "value": False},
        {"key": "Y", "label": "7.0", "value": False},
        {"key": "Z", "label": "ВИП8", "value": False},
        {"key": "AA", "label": "8.0", "value": False},
        {"key": "AB", "label": "Отзыв", "value": False},
        {"key": "AC", "label": "Отзыв", "value": False},
    ],
}


async def snapshot(refresh=False):
    return {
        "ok": True,
        "updated_at": "2026-07-30T15:00:00Z",
        "items": [
            {
                "course_key": "puppy", "course": "Щенок", "stream": "55",
                "curator_value": "Куратор 1", "students_count": 12,
                "students": [
                    {
                        **STUDENT,
                        "row": 8 + index,
                        "enrollment_id": f"preview-{index + 1}",
                        "name": f"Ученик {index + 1}",
                        "email": f"student{index + 1}@example.com",
                        "tariff": ("Стандарт", "Премиум", "ВИП")[index % 3],
                        "responsible_curator": f"Куратор {(index % 3) + 1}",
                    }
                    for index in range(120)
                ],
            },
            {
                "course_key": "puppy", "course": "Щенок", "stream": "56",
                "curator_value": "Куратор 2", "teacher": "Слава", "teacher_id": 2,
                "offer_id": 8593081, "date_start": "2026-08-10", "status": "ready", "students_count": 0, "students": [],
                "vk_link": "https://vk.example/56", "tg_link": "https://t.me/example56",
            },
            {
                "course_key": "dog", "course": "Собака", "stream": "54",
                "curator_value": "Куратор 3", "teacher": "Настасья", "teacher_id": 3,
                "offer_id": 8593084, "date_start": "2026-08-15", "status": "ready", "students_count": 12, "students": [],
                "vk_link": "https://vk.example/54", "tg_link": "https://t.me/example54",
            },
        ],
    }


async def recipients(**kwargs):
    return {"ok": True, "status": "resolved", "telegram": "123456", "vk": "654321", "conflicts": []}


async def successful_managers(**kwargs):
    return {
        "ok": True,
        "items": [
            {"key": item["key"], "manager_name": "Татьяна Воробьева", "manager_id": "42", "deal_id": "873"}
            for item in kwargs.get("identities") or []
        ],
    }


ACCESS_GROUPS = [
    {"group_id": 4059685, "name": "Знакомство. Щенок", "course_key": "puppy", "group_kind": "root", "managed": True},
    {"group_id": 4306384, "name": "Выдача Щенка без процесса", "course_key": "puppy", "group_kind": "bridge", "managed": True},
    {"group_id": 4059659, "name": "Стандарт. Щенок", "course_key": "puppy", "group_kind": "package", "package_key": "standard", "managed": True},
    *[{"group_id": 4059686 + index, "name": f"{index} модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": index, "managed": True} for index in range(10)],
    {"group_id": 3543056, "name": "Тест-драйв. Собака", "course_key": "dog", "group_kind": "root", "managed": True},
    {"group_id": 4306388, "name": "Выдача Собаки без процесса", "course_key": "dog", "group_kind": "bridge", "managed": True},
    {"group_id": 3577198, "name": "Премиум. Собака", "course_key": "dog", "group_kind": "package", "package_key": "premium", "managed": True},
    *[{"group_id": 3763803 + index, "name": f"{index} модуль. Собака", "course_key": "dog", "group_kind": "module", "module_index": index, "managed": True} for index in range(10)],
]


async def access_snapshot(**kwargs):
    return {
        "ok": True,
        "source": "live" if kwargs.get("live") else "cache",
        "updated_at": "2026-08-02T12:00:00Z",
        "groups": [{"group_id": "4059685", "name": "Знакомство. Щенок"}, {"group_id": "4059687", "name": "1 модуль. Щенок"}],
        "requests_left_2h": 74,
    }


async def access_budget():
    return {"requests_left_2h": 74, "needed_for_verification": 6}


async def access_identity(enrollment_id):
    return {"id": enrollment_id, "name": "Ученик", "email": "student@example.com", "gc_user_id": "100500"}


def access_catalog():
    return {"ok": True, "items": ACCESS_GROUPS}


def readiness(course_key, stream):
    return {
        "vk": {"recorded": True, "manageable": False, "status": "legacy_inaccessible"},
        "telegram": {"recorded": True, "manageable": True, "status": "ready"},
    }


def flow_setup():
    return {
        "courses": [{"key": "puppy", "title": "Щенок"}, {"key": "dog", "title": "Собака"}],
        "teachers": [
            {"id": 1, "name": "Ирина", "offer_id": 8593080},
            {"id": 2, "name": "Слава", "offer_id": 8593081},
            {"id": 3, "name": "Настасья", "offer_id": 8593084},
        ],
    }


sys.modules["_nexus_mod_getcourse-chat-fields"] = types.SimpleNamespace(
    service_transfer_snapshot=snapshot,
    service_getcourse_access_snapshot=access_snapshot,
    service_getcourse_access_budget=access_budget,
)
sys.modules["_nexus_mod_messenger-widget"] = types.SimpleNamespace(service_transfer_recipients=recipients)
sys.modules["_nexus_mod_amocrm-db"] = types.SimpleNamespace(service_successful_managers=successful_managers)
sys.modules["_nexus_mod_course-chat-creator"] = types.SimpleNamespace(
    service_transfer_chat_readiness=readiness,
    service_flow_setup=flow_setup,
)
sys.modules["_nexus_mod_chat-moderators"] = types.SimpleNamespace(service_access_catalog=access_catalog)

module._db_path = Path("/tmp/student-transfer-browser-v2.db")
module._module_dir = Path(__file__).resolve().parents[1]
module._snapshot = snapshot
module._access_identity = access_identity


async def admin_user(request):
    return {"username": "browser", "role": "admin"}


module.verify_token_from_request = admin_user

app = FastAPI()
app.include_router(module.router, prefix="/student-transfer/api")
app.mount(
    "/student-transfer/api",
    StaticFiles(directory=str(module._module_dir / "panel"), html=True),
    name="student-transfer-preview",
)
app.mount(
    "/student-transfer/panel",
    StaticFiles(directory=str(module._module_dir / "panel"), html=True),
    name="student-transfer-panel",
)


@app.on_event("startup")
async def startup():
    await module._init_db()
