# Messenger Widget 5.15.0

Release candidate for the week-long production observation period.

- Every initial outbound message is durably queued before the widget confirms acceptance. Provider calls run only in background workers, survive Nexus restarts, and preserve request idempotency.
- `pending`, `processing`, and `retry` deliveries are visible in the employee Operations journal with an animated state, manager, client, message text, and the final human-readable result.
- Provider work is bounded to 45 seconds per attempt. Transient errors use backoff; generic HTTP 429 responses wait at least five minutes, while permanent address/permission errors stop immediately.
- Startup recovers unfinished outbound and amoCRM task jobs. Four outbound workers and bounded GetCourse operation checks prevent one slow provider from blocking other managers.
- Delivery metrics now report non-negative retry counts, end-to-end p50/p95 latency, queue depth, and oldest queue age for messages, amoCRM tasks, and notifications.
- The amoCRM widget remains responsive at desktop and mobile widths and keeps explicit animated loading feedback for all perceptible asynchronous actions.

The release archive intentionally excludes runtime databases, credentials, tokens, logs, tests, and local build output.
