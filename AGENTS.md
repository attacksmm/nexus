# Nexus Project Instructions

These instructions apply to `/home/attack/develop/nexus`.

## Operating Baseline

- Treat Nexus as a compact production service. Keep changes small, explicit, and easy to roll back.
- Prefer adding or updating a module over changing orchestrator core. Touch `main.py`, `orchestrator/`, shared templates, or global static assets only when the module contract cannot solve the task.
- Keep modules self-contained: `manifest.json`, `router.py`, `panel/`, and optional `static/`.
- Do not commit or package secrets. Runtime credentials belong in `.env` and module `env_vars` documentation.
- Preserve unrelated dirty files. This repo often has local work in other modules.

## Module Contract

- Module IDs must remain valid for the existing `ModuleManager` rule: hyphens are allowed and internally converted for identifier validation.
- Public module paths are mounted as:
  - `/nexus/{module_id}/api/...`
  - `/nexus/{module_id}/panel/...`
  - `/nexus/{module_id}/static/...`
- Build installable archives from inside the module directory so the ZIP root contains `manifest.json` and `router.py`.
- Treat `modules/{module_id}/data/` as persistent state independent from the module package. Normal releases must not copy, replace, delete, package, or recreate it.
- Do not back up a module database for a code-only release. Create a database backup only before a schema migration, data repair, bulk mutation, or another operation that can change stored data.

## Telegram Networking Contract

- Production Telegram access is proxy-only. Never call `api.telegram.org` directly and never create a Telethon client from raw ENV parsing inside a module.
- Import the shared helpers from `orchestrator.telegram_proxy` for Bot API base/proxy selection, `httpx` client kwargs, MTProto URL parsing, and Telethon proxy configuration.
- `TELEGRAM_BOT_API_PROXY_URL` is the canonical global Bot API HTTP/SOCKS proxy. `TELEGRAM_MTPROTO_PROXY_URL` is the canonical global MTProto proxy. Legacy module variables are fallback-only and must not override these global values.
- Global Telegram routes are edited and tested in Nexus Settings → Telegram. A change must apply to every Telegram module; long-lived runtimes should implement `async on_telegram_proxy_changed()` to reconnect safely.

## Module Panel UX

- Module panels must follow the existing dark Nexus admin style: black/dark gray surfaces, subtle borders, compact spacing, and no bright browser-default scrollbars.
- Keep panel layouts bounded to the viewport. Long generated code, logs, and docs must live in their own scrollable regions with dark `scrollbar-color`, not force the whole page into awkward horizontal scrolling.
- Generated snippets must be readable and copyable: use wrapped code blocks by default (`white-space: pre-wrap`, `overflow-wrap: anywhere`, `overflow-x: hidden`) and a practical max height so controls remain reachable.
- Tabs and toolbar buttons should have stable dimensions and must not resize when selected, when text wraps, or when content changes.
- Documentation pages must be readable without horizontal scroll on desktop and mobile; long URLs/code examples should wrap.
- For browser-facing modules, verify both the main panel and docs page with Playwright after visual/style changes.

## Validation

- Run the narrowest useful checks before upload: at minimum `python -m py_compile module_x/router.py` and inspect the ZIP contents with `unzip -l`.
- For browser-facing modules, verify the UI with Playwright at a mobile viewport.
- Upload production modules through the Nexus UI when requested, and verify the public endpoint after installation.
- After installation, verify that the uploader preserved the existing runtime `data/` directory. Do not use a database copy as part of a routine version update.
