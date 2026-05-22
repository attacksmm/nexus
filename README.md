# Nexus Orchestrator

Лёгкий оркестратор модулей для сервисных скриптов. Тёмный минималистичный UI: шапка с путём, слева список модулей, справа — интерфейс выбранного модуля (iframe).

## Архитектура

```
nexus/
├── main.py                  # FastAPI app
├── orchestrator/
│   ├── core.py              # ModuleManager — load/unload/pause/resume
│   ├── auth.py              # JWT cookie-аутентификация
│   └── db.py                # SQLite (модули + пользователи)
├── templates/               # login.html, shell.html (Jinja2)
├── static/                  # styles.css, app.js
├── modules/                 # распакованные модули
├── uploads/                 # временные ZIP
└── data/nexus.db            # БД оркестратора
```

## Авторизация

По умолчанию создаётся пользователь `admin` / `admin` (если в БД нет ни одного пользователя). Сменить пароль можно через sqlite напрямую или добавить эндпоинт.

`NEXUS_SECRET` — через переменную окружения (`/home/attack/nexus/.env`).

## Формат модуля (ZIP)

```
my-module.zip
├── manifest.json            # ОБЯЗАТЕЛЬНО: { id, name, version, description? }
├── router.py                # FastAPI router + setup(ctx)
├── panel/
│   └── index.html           # UI модуля (iframe-target)
└── static/                  # опциональная статика (CSS/JS/IMG)
```

### router.py — контракт

```python
from fastapi import APIRouter
import aiosqlite

router = APIRouter()
_ctx = None

def setup(ctx):
    """Вызывается оркестратором при монтировании.
    ctx.module_id  : str
    ctx.module_dir : Path
    ctx.data_dir   : Path (создан автоматически)
    ctx.db_path    : Path к SQLite файлу модуля
    """
    global _ctx
    _ctx = ctx

@router.get("/status")
async def status():
    return {"ok": True}
```

После монтирования модуль доступен:
- API:    `/m/{module_id}/api/...`     ← все маршруты из `router`
- Панель: `/m/{module_id}/panel/...`   ← статика из `panel/`
- Static: `/m/{module_id}/static/...`  ← статика из `static/`

### Состояния

- `active`   — смонтирован, обрабатывает запросы
- `paused`   — на диске есть, но не смонтирован
- `error`    — не удалось загрузить
- `unloaded` — удалён полностью

## Развёртывание

```bash
# на сервере
python3 -m venv /home/attack/nexus/.venv
/home/attack/nexus/.venv/bin/pip install -r /home/attack/nexus/requirements.txt

# .env
echo "NEXUS_SECRET=$(openssl rand -hex 32)" > /home/attack/nexus/.env

# systemd
sudo cp nexus.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nexus
```

Слушает `:8080`. Для production — за nginx с TLS.

## Первый модуль: customer-db

База клиентов с CRUD, поиском по имени/телефону/email/тегам, пагинацией.

API:
- `GET    /m/customer-db/api/customers?q=&limit=&offset=`
- `POST   /m/customer-db/api/customers`
- `GET    /m/customer-db/api/customers/{id}`
- `PUT    /m/customer-db/api/customers/{id}`
- `DELETE /m/customer-db/api/customers/{id}`
- `GET    /m/customer-db/api/stats`

Сборка ZIP:
```bash
cd module_customer_db && zip -r ../customer-db.zip manifest.json router.py panel/
```
