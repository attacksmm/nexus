# Employee-management coverage

This document is the release checklist for the central `staff-registry` module. A
zone is complete only when the registry can discover its current state, apply an
idempotent update, deactivate access without deleting history, and the legacy UI
can no longer create a second source of truth.

| Zone | Exact identity / local link | Behaviour preserved by connector |
| --- | --- | --- |
| Nexus core | `nexus` username / user ID | Login, role, panel access, active state, one-time password |
| Messenger Widget | Messenger admin ID plus GetCourse/amoCRM identities | Role, identity bindings, amoCRM task routing, course-chat notifications and recipients, login codes, device/session revocation |
| Course Chat Creator | VK and Telegram IDs / person ID | Admin/author/curator/tech role, offer, parity, shared VK staff roster |
| Student Transfer | Operator login / operator ID | Operator account, active state, one-time password; flow assignment remains an operational action |
| Sales Chats | Exact login / account ID | Account name, active state and session revocation |
| SBKVD GPT | Exact login / account ID | Account, prompt/model access and defaults, active state and session revocation |
| Email Channel | Messenger link / sender manager ID | Personal sender address and active state |
| Admin Handoff | Exact VK ID | Allowed administrator list and filter used by dialogue protection |
| Chat Moderator | Exact VK and Telegram IDs | Moderator-admin/trusted-sender exceptions and permission to add the Telegram bot |
| Chat Moderators | Exact VK and Telegram IDs | Moderator-admin/trusted-sender exceptions, permission to add the Telegram bot; shared course roster keeps promotion/protection behaviour |
| Bizon → amoCRM | Exact amoCRM user ID | Per-binding responsible round-robin pools and their existing cursors |
| GetCourse → amoCRM | Exact amoCRM user ID | Global round-robin pool and per-binding deal/task responsibles |
| GetCourse chat fields | Registry source link / normalized curator-name marker | Curator aliases from staff sheets and the GetCourse curator field value |

## Deliberate exclusions

- OpenRouter `users` are customer/conversation identities, not employee accounts.
- Responsible IDs stored on historical events, deals, messages and analytics rows
  remain immutable history. The registry changes only configurable employee pools
  and future automation behaviour.
- Choosing a curator for a concrete stream is an operational action in Streams;
  the available curator roster and the employee's role are managed centrally.
- Service credentials, Telegram proxies and external API tokens remain in Nexus
  environment/settings and are never stored in an employee card.

## Safety contract

- Matching is automatic only for an existing source link or an exact provider ID;
  names are never used to merge people.
- Deactivation is soft: access and active sessions are revoked while business
  records and audit history remain.
- Sync uses idempotency keys, retries transient failures, and ignores stale upsert
  jobs after a person has been suspended or offboarded.
- Passwords are passed once to Nexus or Streams and are never persisted in the
  registry database, sync results, or audit log.
- Legacy mutation APIs are disabled only while `staff-registry` is loaded, so
  unloading the module is an immediate rollback path.
