# Nexus security review — 2026-07-29

## Result

The production Nexus host was reviewed as a running system and against the current repository. Low-risk hardening was applied and verified without schema changes or runtime-data replacement. Nexus and nginx are active, the public health check passes, and the focused regression suite reports 27 passing tests.

## Applied

- Disabled public Swagger, ReDoc, and OpenAPI endpoints and restricted accepted Host headers in `main.py:81-91`.
- Added a same-origin check for cookie-authenticated state-changing browser requests in `main.py:118-137` and `main.py:166-174`. Requests without a Nexus cookie remain compatible with existing webhooks and server-to-server clients.
- Added `Cache-Control: no-store` to login, settings, credential, and token-vault responses in `main.py:171-174`.
- Protected Tilda module configuration status and the chat-link debug catalog with panel authorization in `module_tilda_chat_links/router.py:1467-1471` and `module_tilda_chat_links/router.py:1778-1782`. Released as `tilda-chat-links` 1.0.18.
- Stopped nginx access logs from recording query strings, hid the nginx version, and added `nosniff` and a restrictive referrer policy in `ops/nexus-nginx-security.conf:1-3` and `ops/nexus.nginx.conf:1-19`.
- Hardened the systemd service with a private umask, no-new-privileges, empty capabilities, private temporary storage, and protected kernel/system paths in `ops/nexus-hardening.conf:1-17`.
- Changed secret and runtime-state permissions to owner-only. `.env` is `0600`; Nexus/module runtime `data/`, `secrets/`, `uploads/`, and `backups/` expose no group/world permission bits. Existing executable owner bits were preserved.
- Made `.env` reads and atomic writes enforce mode `0600` in `orchestrator/auth.py:392-406`; legacy `.env.bak` is absent.
- Updated `python-multipart` to 0.0.31, `cryptography` to 48.0.1, `pyasn1` to 0.6.4, and production pip to 26.1.2. `pip check` passes.
- Verified production behavior: `/docs`, `/redoc`, and `/openapi.json` return 404; invalid Host returns 400; cross-origin cookie mutation returns 403; same-origin mutation remains accepted; unauthenticated Tilda status/debug return 403; authenticated requests return 200.
- Verified persistent state after the Tilda release: database inode, size, and SHA-256 remained unchanged and SQLite `PRAGMA quick_check` returned `ok`. All 70 discovered SQLite files remained readable; the core database also returned `ok`.

## Remaining risks

### High — framework dependency advisories

Production still uses FastAPI 0.115.12 with Starlette 0.46.2. The installed Starlette version is affected by published denial-of-service advisories, including multipart/form limits and pathological `FileResponse` range processing. A complete dependency-audit resolution requires a major FastAPI/Starlette upgrade and broad endpoint, upload, streaming, and module regression testing. This was not changed unattended because the compatibility risk is materially higher than the current targeted fixes.

Recommended next step: test FastAPI 0.139.2 with Starlette 1.3.1 in a staging copy, then deploy during a monitored window.

### Medium — host account can become root

The Nexus service runs as the `attack` SSH account, and that account has passwordless sudo. `NoNewPrivileges=true` and an empty capability set protect the service process, but compromise of the interactive SSH identity would still permit root access.

Recommended next step: create a dedicated, non-login `nexus` service user and limit deployment sudo commands explicitly. This requires ownership and operational-workflow changes and was therefore not applied unattended.

### Medium — CSRF defense is origin-based

Cookie-authenticated mutations now reject cross-site Fetch Metadata or mismatched Origin headers, and existing cookies use browser SameSite protection. Clients without Origin are intentionally allowed for compatibility. This is a meaningful mitigation but not a per-request synchronizer token.

Recommended next step: add CSRF tokens to Nexus-owned forms and JSON mutation clients after inventorying every module panel.

### Medium — host firewall policy is not enforced locally

UFW is inactive. The observed public listeners are limited to SSH, HTTP, and HTTPS, while Uvicorn listens on loopback. An external/cloud firewall could not be verified from the host.

Recommended next step: confirm the provider firewall, then adopt an explicit inbound policy without interrupting SSH access.

### Medium — residual dependency without a patched release

`ecdsa` 0.19.2 remains installed through `python-jose` and is listed under `PYSEC-2026-1325` without a published fixed version at audit time. Nexus tokens use HS256 rather than ECDSA, reducing direct exposure, but the unused implementation remains present.

Recommended next step: migrate JWT handling to a maintained library that does not pull in `ecdsa`, after compatibility tests.

### Low — secrets remain in a local environment file

Application credentials remain plaintext in `.env`, protected by `0600`, owner-only directories, and the service sandbox. The service account and root can still read them by design.

Recommended next step: move secrets to systemd credentials or a dedicated secret store when service-user separation is implemented.

### Low — historical logs retain old query parameters

New nginx access entries omit query strings. Older access logs may still contain credentials previously passed in URLs. They are access-restricted and were retained to avoid destructive log deletion.

Recommended next step: rotate historical logs under an approved retention policy and revoke any credential known to have appeared in a URL.

### Low — browser policy can be strengthened later

HSTS and a Content Security Policy are not enabled. HSTS changes recovery behavior, and the current panels use inline scripts that need a deliberate CSP migration.

Recommended next step: inventory inline assets, introduce CSP in report-only mode, and enable HSTS only after confirming HTTPS coverage and certificate operations.

## Verification record

- 27 focused tests passed against the production Python environment.
- `pip check` passed.
- systemd security exposure score: 4.7 (`OK`).
- Nexus and nginx: active, zero recorded service restarts, successful result.
- Public `/nexus/healthz`: `{"ok":true}`.
- No schema migration, data repair, or bulk mutation was performed; no database backup was created for this code-only release.
