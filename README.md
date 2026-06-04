# Nexus Orchestrator

Nexus — компактный FastAPI-оркестратор для сервисных модулей. Он даёт единую тёмную админ-панель, cookie-аутентификацию, установку модулей из ZIP, отдельные директории данных для каждого модуля и стабильные URL для API, панели и статики.

Проект используется как рабочая платформа для интеграций, логирования, клиентских таблиц, webhook-обработчиков и файлового хранилища.

## Возможности

- установка и обновление модулей через ZIP;
- запуск, пауза, возобновление и выгрузка модулей;
- роли пользователей: `admin`, `editor`, `viewer`;
- ограничение доступа пользователей к отдельным модулям;
- единый shell-интерфейс с iframe-панелями модулей;
- SQLite-хранилище оркестратора и отдельные SQLite/файловые данные модулей;
- настройка `.env` через админку без показа секретных значений;
- системная и модульная диагностика через модуль `logger`.

## Архитектура

```text
nexus/
├── main.py                  # FastAPI-приложение оркестратора
├── orchestrator/
│   ├── core.py              # ModuleManager: install/load/unload/pause/resume
│   ├── auth.py              # JWT cookie-аутентификация и пользователи
│   └── db.py                # SQLite: modules, users, meta
├── templates/               # Jinja2-страницы shell/login/settings
├── static/                  # глобальные CSS/JS Nexus shell
├── module_*                 # исходники модулей в репозитории
├── *.zip                    # installable ZIP-архивы модулей
├── modules/                 # runtime-распаковка модулей, не хранится в git
├── uploads/                 # временные ZIP при установке, не хранится в git
└── data/nexus.db            # runtime-БД оркестратора, не хранится в git
```

## Production URL

На сервере Nexus работает за nginx с root path `/nexus`.

- shell: `/nexus/`
- настройки: `/nexus/settings`
- API оркестратора: `/nexus/api/...`
- API модуля: `/nexus/{module_id}/api/...`
- панель модуля: `/nexus/{module_id}/panel/index.html`
- документация модуля: `/nexus/{module_id}/panel/docs.html`
- статика модуля: `/nexus/{module_id}/static/...`

При локальном запуске без reverse proxy пути такие же, но без внешнего префикса root path: `/{module_id}/api/...`, `/{module_id}/panel/...`.

## Модули

### `customer-db`

Модуль клиентских таблиц. Поддерживает несколько именованных таблиц, произвольные JSON-поля, CRUD, поиск и статистику.

Основные API:

- `GET /nexus/customer-db/api/tables`
- `POST /nexus/customer-db/api/tables`
- `GET /nexus/customer-db/api/tables/{table}/records`
- `POST /nexus/customer-db/api/tables/{table}/records`
- `PUT /nexus/customer-db/api/tables/{table}/records/{id}`
- `DELETE /nexus/customer-db/api/tables/{table}/records/{id}`

### `logger`

Терминал логов Nexus и установленных модулей. Показывает только реальные модули из БД Nexus, а не служебные backup-каталоги на диске. В каждой вкладке видны обычные строки успеха и ошибки, есть режим «Ошибки» и быстрый поиск по открытому логу.

Основные API:

- `GET /nexus/logger/api/modules`
- `GET /nexus/logger/api/logs/nexus`
- `GET /nexus/logger/api/logs/{module_id}`
- `GET /nexus/logger/api/logs/{module_id}/download`
- `WS /nexus/logger/api/ws/{module_id}`

### `file-storage`

Безопасное файловое хранилище для Nexus. Поддерживает папки, создание текстовых файлов, загрузку разрешённых типов и прямые публичные ссылки на файлы.

Ключевые правила безопасности:

- файлы физически хранятся в `data/blobs/` под UUID-именами;
- пользовательское имя хранится в БД и используется только для отображения/URL;
- публичная ссылка содержит длинный случайный token;
- admin/editor нужны для создания, загрузки, переименования и удаления;
- публичный endpoint отдаёт только конкретный файл по token, без листинга папок;
- запрещены HTML, JS, SVG, исполняемые и shell-файлы;
- лимит одного файла: 100 MB.

Пример публичной ссылки:

```text
/nexus/file-storage/api/f/{token}/{filename}
```

Навигационные ссылки внутри авторизованного Nexus поддерживают человекочитаемый путь:

```text
/nexus/#file-storage/prompts
/nexus/#file-storage/prompts/avito_gpt1.txt
```

### `openrouter`

Модуль генерации ответов через OpenRouter API. Использует промпты из `file-storage`, хранит контекст по неизменяемому `platform_id`, позволяет назначать модель отдельно на каждый prompt и вручную пополнять историю диалога.

Ключевые возможности:

- внешний `POST /nexus/openrouter/api/chat` с Bearer token, который генерируется самим модулем;
- отдельный `POST /nexus/openrouter/api/senler-chat`, который возвращает ответ в формате Senler `vars`/`glob_vars`;
- автоматическое создание `conversation_id`, если он не передан;
- режимы контекста: `0` не читает и не пишет историю, `1` читает краткий контекст без записи, `2` читает краткий контекст и сохраняет вопрос/ответ, `3` читает полный контекст и сохраняет, `4` делает то же, что `3`, и автоматически обновляет краткую сводку;
- при кратком чтении сохранённая сводка клиента используется в приоритете вместо истории сообщений;
- глобальная модель по умолчанию и override модели для каждого prompt-файла;
- поиск клиентов по `platform_id` и `conversation_id`;
- просмотр всех пар вопрос/ответ в аккуратной таблице;
- ручное добавление пары вопрос/ответ через API или панель;
- генерация и сохранение краткой сводки по диалогу выбранной моделью.
- вкладка «Протестировать» для panel-only запросов без записи в историю;
- `GET /nexus/openrouter/api/schema` для получения полей API и примеров;
- логирование успешных и ошибочных `/chat` запросов в модульный лог `openrouter`.

Основной запрос:

```http
POST /nexus/openrouter/api/chat
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
Content-Type: application/json

{
  "platform_id": "vk_123",
  "conversation_id": null,
  "prompt": "prompts/avito_gpt1.txt",
  "message": "Вопрос клиента",
  "context": 2
}
```

Запрос для Senler webhook использует те же поля, но возвращает переменные для блока «Обработать ответ в переменные»:

```http
POST /nexus/openrouter/api/senler-chat
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
Content-Type: application/json

{
  "platform_id": 123456,
  "conversation_id": null,
  "prompt": "prompts/avito_gpt1.txt",
  "message": "Вопрос клиента",
  "context": 2
}
```

Ответ:

```json
{
  "vars": [
    {"n": "ai_answer", "v": "Ответ модели"},
    {"n": "conversation_id", "v": "or_conv_..."},
    {"n": "platform_id", "v": "123456"}
  ],
  "glob_vars": []
}
```

Имена переменных можно переопределить в теле запроса: `answer_var`, `conversation_id_var`, `platform_id_var`, `model_var`, `summary_var`, `summary_error_var`. Пустое имя отключает запись конкретной переменной.

Ручное пополнение контекста:

```http
POST /nexus/openrouter/api/context/append
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
Content-Type: application/json

{
  "platform_id": "vk_123",
  "conversation_id": "or_conv_...",
  "question": "1) вопрос пользователя",
  "answer": "1) ответ"
}
```

Для работы нужен OpenRouter API key:

- `OPENROUTER_API_KEY` — API ключ OpenRouter.

`OPENROUTER_API_KEY` можно задать во вкладке «Настройки» самого модуля: Nexus сохранит ключ в `.env` и загрузит его в окружение процесса. Токен для бота генерируется самим модулем, отображается в настройках и может быть перегенерирован без раскрытия OpenRouter API key.

Контекст можно получить отдельно:

```http
GET /nexus/openrouter/api/context/brief?conversation_id=or_conv_...
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
```

Поля API:

```http
GET /nexus/openrouter/api/schema
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
```

```http
GET /nexus/openrouter/api/context/full?conversation_id=or_conv_...
Authorization: Bearer <ТОКЕН МОДУЛЯ ИЗ НАСТРОЕК>
```

### `getcourse-orders`

Webhook-модуль GetCourse. Принимает события заказов, нормализует состояние оплаты, пишет данные в `customer-db` и применяет правила распределения по группам Senler.

Актуальные state-specific endpoints:

- `POST /nexus/getcourse-orders/api/webhook/created`
- `POST /nexus/getcourse-orders/api/webhook/partial`
- `POST /nexus/getcourse-orders/api/webhook/paid`

Legacy endpoint `/webhook` остаётся для status-based обработки.

### `amocrm-senler`

Интеграция amoCRM и Senler. Обрабатывает webhook-события сделок, поддерживает привязки статусов, exclusive-группы, запись переменных Senler и заметки в amoCRM.

### Прочие модули

- `senler` — списки Senler и клиентские трекинг-сценарии;
- `tilda-chat-links` — генерация клиентских ссылок на чаты;
- `course-chat-creator` — создание учебных чатов;
- `salebot-senler-button` — интеграция кнопки Salebot/Senler.

## Формат модуля

Installable ZIP должен содержать файлы в корне архива:

```text
my-module.zip
├── manifest.json
├── router.py
├── panel/
│   ├── index.html
│   └── docs.html            # желательно
└── static/                  # опционально
```

Минимальный `manifest.json`:

```json
{
  "id": "my-module",
  "name": "Мой модуль",
  "version": "1.0.0",
  "description": "Короткое описание модуля"
}
```

`router.py`:

```python
from fastapi import APIRouter

router = APIRouter()
_ctx = None

def setup(ctx):
    """Вызывается оркестратором при монтировании модуля."""
    global _ctx
    _ctx = ctx
    # ctx.module_id  -> str
    # ctx.module_dir -> Path к runtime-директории модуля
    # ctx.data_dir   -> Path к сохранённым данным модуля
    # ctx.db_path    -> Path к SQLite-файлу модуля
    # ctx.logger     -> RotatingFileHandler logger модуля

@router.get("/status")
async def status():
    return {"ok": True}
```

После монтирования все маршруты `router` доступны под `/nexus/{module_id}/api/...`.

## Сборка ZIP

Если установлен `zip`:

```bash
cd module_file_storage
zip -r ../file-storage.zip manifest.json router.py panel/ static/
```

Универсальный вариант без системного `zip`:

```bash
python3 - <<'PY'
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

module = "module_file_storage"
out = Path("file-storage.zip")
base = Path(module)

with ZipFile(out, "w", ZIP_DEFLATED) as zf:
    for path in sorted(base.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            zf.write(path, path.relative_to(base).as_posix())
print(out)
PY
```

Перед публикацией модуля минимум проверить:

```bash
python3 -m py_compile module_file_storage/router.py
python3 - <<'PY'
from zipfile import ZipFile
with ZipFile("file-storage.zip") as zf:
    print("\n".join(zf.namelist()))
PY
```

## Развёртывание

```bash
python3 -m venv /home/attack/nexus/.venv
/home/attack/nexus/.venv/bin/pip install -r /home/attack/nexus/requirements.txt

cat > /home/attack/nexus/.env <<'EOF'
NEXUS_SECRET=replace-with-long-random-secret
EOF

sudo cp /home/attack/nexus/nexus.service /etc/systemd/system/nexus.service
sudo systemctl daemon-reload
sudo systemctl enable --now nexus
```

Сервис слушает `0.0.0.0:8080`. Для production он должен работать за nginx и TLS.

## Безопасность и эксплуатация

- `.env`, `data/`, `modules/`, `uploads/`, SQLite-БД и runtime-файлы не коммитятся.
- Секреты задаются через `.env` или системное окружение.
- Доступ к shell и административным API идёт через `nexus_token` cookie.
- Публичные endpoints должны быть явными и узкими: например, file-storage отдаёт только файл по token.
- Модули должны валидировать имена файлов, table names, module IDs и внешние payloads.
- При обновлении модуля сохраняйте `modules/{module_id}/data`.

## Состояния модулей

- `active` — модуль смонтирован и обрабатывает запросы;
- `paused` — файлы модуля есть на диске, но маршруты не смонтированы;
- `error` — модуль не удалось загрузить;
- `unloaded` — модуль выгружен и удалён из БД.

## Рабочие соглашения

- Модуль должен быть самодостаточным: `manifest.json`, `router.py`, `panel/`, опционально `static/`.
- UI модулей должен соответствовать тёмному компактному стилю Nexus.
- Изменения в оркестраторе делаются только если контракт модуля не решает задачу.
- Для browser-facing модулей проверяйте панель и документацию на desktop и mobile viewport.
