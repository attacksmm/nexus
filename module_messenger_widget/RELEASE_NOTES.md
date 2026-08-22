# Messenger Widget 5.15.3

Release candidate for the week-long production observation period.

## 5.15.3 — reliable templates and image drafts

- Template menus paint the employee's last successful list immediately and refresh it in the background.
- A pending template request is rendered as a spinner, never as the false state “Шаблонов нет”.
- amoCRM and GetCourse composers accept JPG, PNG, GIF, and WebP images from the file picker or clipboard; upload progress remains visible until the draft is usable.
- Uploaded images are size- and signature-checked, stored under random public URLs, and limited to 8 MB.
- The send API now renders template variables itself and applies automatic UTM markup, so an old browser tab cannot send literal `{{utm.*}}` markers.

## 5.15.2 — instant repeat opens

- The last usable card snapshot is painted immediately when the same employee reopens a deal.
- Profiles, channels, and the latest conversation refresh independently in the background.
- A visible animated status explains that saved data is shown while Nexus updates it.
- Cache is isolated per employee, expires after 30 minutes, and is bounded to eight recent cards.
- Failed refreshes no longer erase a usable cached conversation.

- Every initial outbound message is durably queued before the widget confirms acceptance. Provider calls run only in background workers, survive Nexus restarts, and preserve request idempotency.
- `pending`, `processing`, and `retry` deliveries are visible in the employee Operations journal with an animated state, manager, client, message text, and the final human-readable result.
- Provider work is bounded to 45 seconds per attempt. Transient errors use backoff; generic HTTP 429 responses wait at least five minutes, while permanent address/permission errors stop immediately.
- Startup recovers unfinished outbound and amoCRM task jobs. Four outbound workers and bounded GetCourse operation checks prevent one slow provider from blocking other managers.
- Delivery metrics now report non-negative retry counts, end-to-end p50/p95 latency, queue depth, and oldest queue age for messages, amoCRM tasks, and notifications.
- Health and settings reuse the background identity-index snapshot instead of recounting hundreds of thousands of identities per request, keeping simultaneous widget opens responsive.
- The amoCRM widget remains responsive at desktop and mobile widths and keeps explicit animated loading feedback for all perceptible asynchronous actions.

The release archive intentionally excludes runtime databases, credentials, tokens, logs, tests, and local build output.
