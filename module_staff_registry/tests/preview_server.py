from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[1]
app = FastAPI()

EMPLOYEE = {
    "id": "preview-1", "full_name": "Мария Реестрова", "display_name": "Мария",
    "job_profile": "sales_manager", "status": "active", "email": "maria@example.com",
    "phone": "+79991112233", "note": "Отдел продаж", "version": 3,
    "roles": [{"role": "sales_manager", "scope": ""}],
    "identities": [
        {"provider": "telegram", "external_id": "551122", "username": "maria_sales"},
        {"provider": "amocrm", "external_id": "9007", "username": ""},
    ],
    "memberships": [
        {"module_id": "nexus-core", "desired_enabled": True, "config": {"username": "maria.reestrova", "role": "editor", "module_access": ["messenger-widget", "sales-chats"]}, "local_id": "18", "sync_status": "applied", "last_error": ""},
        {"module_id": "messenger-widget", "desired_enabled": True, "config": {"role": "employee", "amo_task_enabled": True}, "local_id": "42", "sync_status": "applied", "last_error": ""},
        {"module_id": "email-channel", "desired_enabled": True, "config": {"local_part": "maria.sales"}, "local_id": "42", "sync_status": "needs_input", "last_error": "Проверьте домен отправителя"},
    ],
    "source_links": {"nexus-core": {"local_id": "18"}, "messenger-widget": {"local_id": "42"}},
}

MODULES = [
    {"module_id": "nexus-core", "label": "Nexus", "description": "Вход в Nexus, роль и доступные панели.", "available": True, "fields": [{"key": "username", "label": "Логин", "type": "text"}, {"key": "role", "label": "Роль", "type": "select", "options": ["viewer", "editor", "admin"]}, {"key": "module_access", "label": "Доступные модули", "type": "list"}]},
    {"module_id": "messenger-widget", "label": "Messenger", "description": "Диалоги, задачи amoCRM и уведомления.", "available": True, "fields": [{"key": "role", "label": "Роль", "type": "select", "options": ["employee", "admin"]}, {"key": "amo_task_enabled", "label": "Создавать задачи amoCRM", "type": "bool"}]},
    {"module_id": "course-chat-creator", "label": "Учебные чаты", "description": "Роль в чатах и настройки куратора.", "available": True, "fields": [{"key": "kind", "label": "Роль", "type": "select", "options": ["admin", "kurator", "author", "tech"]}, {"key": "vk_id", "label": "VK ID", "type": "text"}]},
    {"module_id": "student-transfer", "label": "Streams", "description": "Аккаунт оператора управления потоками.", "available": True, "fields": [{"key": "login", "label": "Логин", "type": "text"}]},
    {"module_id": "sales-chats", "label": "Чаты продаж", "description": "Аккаунт рабочего чата.", "available": True, "fields": [{"key": "login", "label": "Логин", "type": "text"}]},
    {"module_id": "sbkvd-gpt", "label": "SBKVD GPT", "description": "Модели и промпты сотрудника.", "available": True, "fields": [{"key": "login", "label": "Логин", "type": "text"}, {"key": "models", "label": "Модели", "type": "list"}]},
    {"module_id": "email-channel", "label": "Email", "description": "Персональный адрес отправителя.", "available": True, "fields": [{"key": "local_part", "label": "Адрес до @", "type": "text"}]},
]


@app.get("/")
def root():
    return FileResponse(ROOT / "panel" / "index.html")


@app.get("/api/capabilities")
def capabilities():
    return {"ok": True, "can_manage": True, "profiles": {
        "sales_manager": {"label": "Менеджер продаж", "roles": ["sales_manager"], "modules": {"nexus-core": {"role": "editor"}, "messenger-widget": {"role": "employee"}, "sales-chats": {}}},
        "curator": {"label": "Куратор", "roles": ["curator"], "modules": {"course-chat-creator": {"kind": "kurator"}, "student-transfer": {}}},
        "custom": {"label": "Настроить вручную", "roles": [], "modules": {}},
    }, "modules": MODULES}


@app.get("/api/employees")
def employees():
    return {"ok": True, "items": [{**{k: v for k, v in EMPLOYEE.items() if k not in {"roles", "identities", "memberships", "source_links"}}, "module_count": 3, "problem_count": 1}]}


@app.get("/api/employees/{employee_id}")
def employee(employee_id: str):
    return {"ok": True, "employee": EMPLOYEE}


@app.get("/api/health")
def health():
    return {"ok": True, "employees": 1, "pending_jobs": 1, "worker": True}


@app.get("/api/discovery")
def discovery():
    return {"ok": True, "items": [{"candidate_key": "preview-candidate", "module_id": "course-chat-creator", "local_id": "77", "match_employee_id": "", "match_reason": "", "status": "new", "payload": {"full_name": "Анна Куратор", "display_name": "Анна", "active": True}}]}


@app.get("/api/jobs")
def jobs():
    return {"ok": True, "items": [{"id": "job-1", "employee_id": "preview-1", "module_id": "email-channel", "operation": "upsert", "status": "needs_input", "error": "Проверьте домен отправителя", "created_at": "2026-09-02T07:10:00+00:00"}]}


@app.get("/api/audit")
def audit():
    return {"ok": True, "items": [{"id": 1, "employee_id": "preview-1", "actor": "admin", "action": "employee_updated", "details": {"modules": ["nexus-core", "messenger-widget"]}, "created_at": "2026-09-02T07:10:00+00:00"}]}


@app.api_route("/api/{path:path}", methods=["POST", "PUT", "DELETE"])
def mutate(path: str):
    return JSONResponse({"ok": True, "employee": EMPLOYEE, "jobs": ["preview-job"], "employee_id": "preview-1", "discovered": 1})


app.mount("/", StaticFiles(directory=ROOT / "panel", html=True), name="panel")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8767)
