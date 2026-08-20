import json
import sys
import types
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from module_student_transfer import router as module


STUDENT = {
    "enrollment_id": "preview-ivan",
    "row": 8,
    "name": "Иван Петров",
    "email": "ivan@example.com",
    "phone": "+7 926 555-00-07",
    "gc_user_id": "100500",
    "order_id": "200600",
    "deal_number": "D-200600",
    "tg_account": "@ivan_sobakovod",
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
                "vk_link": "https://vk.example/55", "tg_link": "https://t.me/example55",
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
                "vk_admin_url": "https://vk.ru/gim123?sel=c56",
            },
            {
                "course_key": "dog", "course": "Собака", "stream": "54",
                "curator_value": "Куратор 3", "teacher": "Настасья", "teacher_id": 3,
                "offer_id": 8593084, "date_start": "2026-08-15", "status": "ready", "students_count": 12, "students": [],
                "vk_link": "https://vk.example/54", "tg_link": "https://t.me/example54",
                "vk_admin_url": "https://vk.ru/gim123?sel=c54",
            },
        ],
    }


async def recipients(**kwargs):
    return {"ok": True, "status": "resolved", "telegram": "123456", "vk": "654321", "conflicts": []}


async def delivery_target(**kwargs):
    return {
        "ok": True,
        "provider": "salebot",
        "recipient_id": "123456",
        "channel": "TG",
    }


async def messenger_conversations(**kwargs):
    return {
        "ok": True,
        "channels": [
            {
                "channel_id": "max:preview", "transport": "max", "provider": "wazzup",
                "label": "MAX · Служба заботы", "chat_id": "", "has_chat": False, "can_send": True,
                "messages": [],
            },
            {
                "channel_id": "vk:preview", "transport": "vk", "provider": "vk",
                "label": "VK · Кинолог Анна Тимофеева. Современный собаковод.", "chat_id": "", "has_chat": False,
                "can_send": False, "send_reason": "Клиент не связан с этим каналом", "messages": [],
            },
            {
                "channel_id": "telegram-personal:preview", "transport": "telegram", "provider": "telegram_personal",
                "label": "Telegram Personal · 5601500901", "chat_id": "123456", "has_chat": True, "can_send": True,
                "messages": [
                    {"direction": "incoming", "text": "Здравствуйте! Когда начинается поток?", "sent_at": "2026-08-13T08:15:00Z"},
                    {"direction": "outgoing", "text": "Здравствуйте! Старт уже указан в карточке потока.", "sent_at": "2026-08-13T08:17:00Z"},
                ],
            },
            {
                "channel_id": "salebot:project", "transport": "salebot", "provider": "salebot",
                "label": "SaleBot · Проект", "chat_id": "1007094687", "has_chat": True, "can_send": True,
                "messages": [
                    {"direction": "incoming", "text": "У меня вопрос по занятию", "sent_at": "2026-08-13T08:18:00Z"},
                    {"direction": "outgoing", "text": "Конечно, расскажите подробнее.", "sent_at": "2026-08-13T08:19:00Z", "attachments": [
                        {"content_uri": "/student-transfer/panel/preview-photo.svg", "content_type": "image/svg+xml", "filename": "Фото с занятия.png"},
                        {"content_uri": "https://example.test/guide.pdf", "content_type": "document", "filename": "Памятка.pdf"},
                    ]},
                ],
            },
        ],
        "templates": [
            {"id": 1, "title": "Ссылка на чат", "body": "Здравствуйте! Вот ссылка на чат вашего потока: {chat_link}", "scope": "shared", "folder": "Доступы", "favorite": True, "favorite_order": 0},
            {"id": 2, "title": "Доступ выдан", "body": "Готово — доступ к курсу выдан.", "scope": "personal", "folder": "Ответы", "favorite": False},
        ],
        "profile_links": [
            {"kind": "vk", "label": "VK: Алина Соколова", "url": "https://vk.com/id123456"},
            {"kind": "salebot", "label": "SaleBot: Алина в SaleBot", "url": "https://salebot.pro/projects/397724/clients/123456"},
        ],
        "send_all_default": False,
    }


async def messenger_send(**kwargs):
    return {"ok": True, "status": "sent", "channel": kwargs.get("channel"), "text": kwargs.get("text")}


async def messenger_template_preview(**kwargs):
    return {"ok": True, "text": kwargs.get("body") or "Здравствуйте! Данные клиента подставлены.", "missing": []}


async def messenger_template_favorite(**kwargs):
    return {"ok": True, "id": kwargs.get("template_id"), "favorite": kwargs.get("favorite")}


async def flow_sheet_preflight(**kwargs):
    return {
        "ok": True,
        "sheet_title": f"С{kwargs.get('stream')} (13.08)",
        "template_title": "С55 (03.08)",
    }


async def creator_status():
    return {
        "ok": True,
        "required_env": {
            "vk_group_token": True,
            "vk_group_id": True,
            "telegram_api": True,
            "telegram_session": True,
        },
        "chat_links_sync": {"configured": True},
    }


async def successful_managers(**kwargs):
    return {
        "ok": True,
        "items": [
            {"key": item["key"], "manager_name": "Татьяна Воробьева", "manager_id": "42", "deal_id": "873", "deal_url": "https://sobakovodpro.amocrm.ru/leads/detail/873"}
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
    {"group_id": 4842617, "name": "Мини-курс Намордник", "course_key": "mini_muzzle", "group_kind": "mini", "managed": True},
    {"group_id": 4842619, "name": "Мини-курс Намордник с проверкой и поддержкой", "course_key": "mini_muzzle", "group_kind": "mini", "managed": True},
    {"group_id": 4119459, "name": "Мини-курс Поводок", "course_key": "mini_leash", "group_kind": "mini", "managed": True},
    {"group_id": 4217019, "name": "Мини-курс Послушание", "course_key": "mini_obedience", "group_kind": "mini", "managed": True},
    {"group_id": 4443745, "name": "За 15 минут", "course_key": "mini_15", "group_kind": "mini", "managed": True},
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


def latest_access_verification(**kwargs):
    return {"ok": True, "pending": False}


def access_verifications(**kwargs):
    item = {
        "request_id": "preview-access-1",
        "requester_user_id": "1",
        "identifier": "student1@example.com",
        "gc_user_id": "100500",
        "status": "applied",
        "created_at": 1786957200,
        "applied_at": 1786957260,
        "current_groups": [{"name": "Знакомство. Щенок"}],
        "target_groups": [{"name": "Знакомство. Щенок"}, {"name": "1 модуль. Щенок"}],
        "apply_result": {
            "verification_pending": True,
            "verification_attempts": 1,
            "verification_next_at": 1786957500,
            "verification_error": "GetCourse обновляет список групп",
        },
    }
    if kwargs.get("request_id") and kwargs["request_id"] != item["request_id"]:
        return {"ok": True, "items": []}
    return {"ok": True, "items": [item]}


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
    service_registry_flow_sheet_preflight=flow_sheet_preflight,
)
sys.modules["_nexus_mod_messenger-widget"] = types.SimpleNamespace(
    service_transfer_recipients=recipients,
    service_transfer_delivery_target=delivery_target,
    service_streams_conversations=messenger_conversations,
    service_streams_send=messenger_send,
    service_streams_template_preview=messenger_template_preview,
    service_streams_template_favorite=messenger_template_favorite,
)
sys.modules["_nexus_mod_amocrm-db"] = types.SimpleNamespace(service_successful_managers=successful_managers)
sys.modules["_nexus_mod_course-chat-creator"] = types.SimpleNamespace(
    service_transfer_chat_readiness=readiness,
    service_flow_setup=flow_setup,
    status=creator_status,
)
sys.modules["_nexus_mod_chat-moderators"] = types.SimpleNamespace(
    service_access_catalog=access_catalog,
    service_latest_access_verification=latest_access_verification,
    service_access_verifications=access_verifications,
)

module._db_path = Path("/tmp/student-transfer-browser-v3.db")
module._module_dir = Path(__file__).resolve().parents[1]
module._snapshot = snapshot
module._access_identity = access_identity


async def admin_user(request):
    return {
        "id": 1, "username": "browser", "role": "admin",
        "login": "Никита Попов", "display_name": "Никита Попов",
    }


module.verify_token_from_request = admin_user
module._require_operator = admin_user

app = FastAPI()
app.include_router(module.router, prefix="/student-transfer/api")
app.include_router(module.router, prefix="/student-transfer/panel")


@app.get("/student-transfer/panel/preview-photo.svg")
async def preview_photo():
    return Response(
        """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 360">
        <defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
        <stop stop-color="#f97316"/><stop offset="1" stop-color="#7c2d12"/></linearGradient></defs>
        <rect width="720" height="360" rx="24" fill="url(#g)"/>
        <circle cx="132" cy="132" r="54" fill="#fff" fill-opacity=".86"/>
        <path d="M52 314l158-142 86 76 92-91 184 157z" fill="#fff" fill-opacity=".72"/>
        <text x="360" y="334" text-anchor="middle" font-family="sans-serif" font-size="24" fill="#fff">Фото вложения</text>
        </svg>""",
        media_type="image/svg+xml",
        headers={"Cache-Control": "no-store"},
    )


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
    async with module._connect() as db:
        await db.execute(
            """INSERT OR REPLACE INTO flow_jobs(
               id,status,course_key,stream,date_start,teacher_id,operator_id,operator_name,result_json,error,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "preview-dog-56", "attention", "dog", "56", "2026-08-13", 1, 1, "Андрей Карачкиев",
                json.dumps({
                    "stages": {"sheet": "completed", "chats": "completed", "links": "completed", "sync": "completed", "manual_vk": "running"},
                    "create": {
                        "vk": {"chat_id": 88, "owner_group_id": 225075265, "group_link": "https://vk.me/join/temporary"},
                        "telegram": {"group_link": "https://t.me/+dog56"},
                    },
                    "sync": {"reconciled": [{"email": f"student-{index}@example.com"} for index in range(1000)]},
                }, ensure_ascii=False),
                "Создание ещё не закончено: нужно вручную настроить VK-чат.",
                "2026-08-13T07:08:04Z", "2026-08-13T07:08:04Z",
            ),
        )
        await db.commit()
