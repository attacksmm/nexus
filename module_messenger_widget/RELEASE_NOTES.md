# Messenger Widget

## 5.20.20 — channel readiness and trial summaries

- Expired known channel catalogues render immediately while the shared refresh runs.
- Telegram shows an explicit phone lookup action when no lookup is running, not a misleading pending spinner.
- MAX/Telegram recipient restrictions reuse the card database connection.
- Unpaid GC summaries identify an active recorded trial separately from purchased access; status lookup is bounded to one second and never grants/revokes access.

## 5.20.19 — indexed card reads under production load

- Channel presence uses separate phone/exact-route indexed reads, never an unindexed OR across all chats.
- Owner assignment and saved provider names use exact channel/type/client keys; unchanged owners are not rewritten.
- Conversation lookup unions indexed phone and exact-chat matches while retaining history and deduplication.
- Automatic VK polling no longer runs a historical placeholder-name repair every five minutes. Explicit full sync retains this maintenance action.
- No schema migration or changes to provider delivery and authentication contracts.

## 5.20.18 — быстрые каналы, точные статусы и полная видеоинструкция

- amoCRM открывает Telegram Personal без долгого синхронного перебора диалогов; проверка нового получателя выполняется при отправке с ограниченными таймаутами.
- Явно недоступный получатель Telegram/MAX отключается только для этого клиента и канала; сетевые и rate-limit ошибки остаются повторяемыми.
- Отправка сразу подтверждает постановку в надёжную очередь, а сотрудник видит отдельные состояния передачи каналу, доставки, прочтения и понятной ошибки.
- Ранние и переставленные Wazzup callback согласуются с сообщением и outbound job монотонно, включая короткое окно перезапуска процесса.
- Общий список диалогов снова запускается на странице сотрудников GetCourse, не добавляя фоновой нагрузки карточкам клиентов.
- Каждый из 18 разделов инструкции содержит запись настоящего интерфейса с плавным курсором, кликами, вводом, субтитрами и явным состоянием загрузки/повтора.

## 5.20.15 — responsive conversations and safe asynchronous updates

- Saved VK messages render before provider refresh; concurrent history readers share one cancellable-by-lifecycle provider task.
- Conversation/send reuse one channel catalogue; saved Wazzup history avoids unnecessary identity resolution.
- amoCRM send results no longer wait for history refresh. Confirmation locks, captured context/subject and stale-response guards protect channel changes and newer drafts.
- GC preserves per-conversation drafts, joins first-page requests, shows retry states and releases its invisible overlay on close.
- Background polling preserves older-history reading and cannot re-enable a busy send button.
- Validated with 189 Python tests, eight asynchronous UI tests and desktop/mobile browser fixtures. Live customer delivery was not exercised.

Release candidate for the week-long production observation period.

## 5.19.3 — automatic amoCRM conversation links

- New amoCRM leads receive their protected `Переписка` URL in the background within about one minute, without opening the desktop widget.
- The synchronizer scans by creation time with a five-minute overlap, skips exact existing links, rate-limits writes, and stores a durable cursor only after a complete page range.
- amoCRM PATCH bodies contain only `custom_fields_values` for the URL field; status, pipeline and responsible user are never submitted.
- Health now reports the last mobile-link synchronization state and counts for production verification.

## 5.19.2 — protected full-screen amoCRM conversations on Android

- Every opened amoCRM lead receives a stable signed URL in the custom field `Переписка`; the remote interface updates it without another amoCRM ZIP upload.
- The URL serves no client data before authentication, is excluded from indexing and caching, and rejects modified or enumerated lead IDs.
- A new Android device requires the employee's persistent Nexus activation code and an active amoCRM staff binding.
- The live lead and contact context is loaded server-side from amoCRM. Administrators may open any lead; employees may open only leads currently assigned to their own bound amoCRM user.
- The full-screen workspace reuses the same channels, history, Email guardrails, templates, drafts and delivery behavior as the desktop amoCRM widget.
- Real manager notifications include a protected `Открыть переписку` action; bare time-button answers after a proven automation-authored funnel message stay silent, while the same answer to a manager remains actionable.

## 5.19.1 — exact reopen and composer alignment

- The amoCRM bootstrap keeps the same-card iframe alive while the modal is closed, so the selected Email channel, subject, draft, scroll and attachment stay exactly where the operator left them.
- A fresh same-card snapshot is restored without another channel discovery request, including while an older amoCRM bootstrap is still installed.
- Reopening GetCourse resumes its existing polling timer without an immediate duplicate conversation request.
- Textareas are block-level inside their composer wrappers, making the Send button exactly the same height as the visible message field; empty error rows no longer add stray space.

## 5.19.0 — stable reopen and channel order

- amoCRM and GetCourse keep the selected channel, draft, Email subject and scroll position when the same card widget is closed and reopened.
- amoCRM waits briefly for the complete contact context instead of painting and caching a provisional channel list; background polling pauses while the widget is hidden.
- The channel catalogue uses stale-while-revalidate and never drops Email merely because a provider refresh exceeds the first-paint deadline.
- Channel keys and ordering are stable across refreshes; stale responses can no longer overwrite a newer GetCourse card.
- Email actions stay inside the message editor, and the Send button stretches to the bottom of the message field on both surfaces.
- A GetCourse profile link is suppressed for an active staff account unless that person also has real paid student access.
- Email is added once to every employee's existing amoCRM task-source selection, while preserving every source they selected or disabled before the release.
- Short replies to an automated funnel message or voice note no longer create a notification or amoCRM task; the same reply to a manager remains actionable, and meaningful client text is never hidden by this rule.

## 5.18.7 — confirmed means confirmed

- A queued email remains visible in history but no longer promotes Email to the established-dialog tier until the provider accepts it or the client replies.

## 5.18.6 — channel confidence and first-email guidance

- Channel tabs are ordered by delivery confidence: an established dialog first, then an exact available destination, a best-effort lookup/delivery, and unavailable channels last.
- The email deliverability checklist is required only before the first outgoing message in a thread; replies no longer interrupt the operator with the same confirmation.
- The branded `sobakovod.pro` link in every email signature receives the same card-specific UTM and attribution parameters as links in the message body.

## 5.18.5 — remotely updatable amoCRM interface

- The amoCRM archive is now a stable bootstrap: normal widget HTML, CSS and client-logic releases arrive with the Nexus module without another amoCRM ZIP upload.
- A five-minute rolling cache key keeps repeat opens fast while making a newly deployed interface visible within five minutes.
- Re-uploading the amoCRM ZIP remains necessary only for bootstrap, manifest, permission, placement or amoCRM card-context changes.

## 5.18.4 — protected email sending

- amoCRM, GetCourse cards and the shared inbox show the same two-action deliverability checklist before every manual email.
- The shared email service rejects old clients that omit the current acknowledgement, so the confirmation cannot be bypassed through a stale tab or direct widget request.
- Only direct HTTPS links on `sobakovod.pro` and its subdomains are accepted; shorteners, external or nested redirects, bare domains and client personal data in URLs are blocked.
- The first accepted conversation starts with at most one body link and no attachments. Failed or merely queued attempts do not relax that rule.
- Unsubscribe, bounce and spam-complaint events cancel queued/retry work and are checked again immediately before DashaMail submission.
- Numeric attribution IDs remain valid, and normal template rendering plus personalized UTM completion still runs before the guardrail check.

## 5.18.3 — reliable email context

- Preserves the exact amoCRM/GetCourse card coordinates while exposing the resolved contact email, phone and name to channel services.
- Email no longer reports that the address is missing after identity resolution has already found it in the linked amoCRM contact or GetCourse user.
- Initial conversation loading automatically retries one transient timeout in both widgets while keeping an explicit spinner visible.

## 5.18.0 — pilot email channel

- Adds the optional Nexus `email-channel` transport to amoCRM, GetCourse and Streams while keeping email excluded from “Отправить везде”.
- A first email requires a subject; replies inherit the thread subject and show compact queued, sent, delivered, opened and failed states.
- Incoming email tasks are fail-closed: Email is disabled for every employee until explicitly selected, and the email module starts in shadow mode.
- Exact entity bindings prevent a shared address from attaching one thread to two amoCRM leads.

## 5.17.7 — hidden personal Telegram dialogs

- Personal Telegram accounts `@papaproduser` and `@Rareru` are excluded by stable peer identifiers, with phone/username aliases as an additional boundary.
- The accounts are skipped by background and realtime synchronization, omitted from the shared inbox and GetCourse Streams view, and cannot be opened or messaged through a direct widget request.
- Existing stored history remains untouched but is no longer returned to amoCRM or GetCourse widgets.

## 5.17.4 — Faster concurrent cards and inbox

- Reuses one card-local SQLite connection for direct-channel lookups without serializing employees.
- Loads inbox identities with the main query and batches link updates into one transaction.

## 5.17.3 — GetCourse card parity

- GetCourse source fields are read from both table rows and the block layout used by current user cards, so `utm_term` participates in VK identity and template resolution.
- Profile links now appear in the GetCourse header; the GetCourse access editor has a dedicated action beside settings with a visible loading/error state.
- VK can be opened through a verified `utm_term`, and Telegram Personal can attempt an explicit phone lookup from a GetCourse card.
- Removed the redundant in-chat Channels/Open Wazzup toolbar and the empty attachment strip below the composer.

## 5.17.2 — MAX communication history

- The MAX filter now resolves the durable Wazzup/MAX transport rows, including MAX group dialogs, instead of looking for a nonexistent standalone provider value.
- MAX rows are labeled `MAX` in the communication history rather than the internal `wazzup` provider name.

## 5.17.1 — finite refresh indicators

- A staged amoCRM card context now owns and always clears its channel refresh state, including when it supersedes the first request or the enrichment request fails.
- Background profile discovery continues without rendering a permanent spinner in the profile-link header.

## 5.17.0 — clear delivery states and faster first paint

- Outgoing messages use compact accessible states instead of raw provider words: a green check for sent, a red cross for delivery failure, and a blue eye when the provider confirms that the message was read.
- amoCRM paints the exact deal and any safe cached conversation immediately while the full contact details continue loading in the background.
- The background amoCRM enrichment updates the existing view without clearing a draft message.
- GetCourse preloads channels while the employee views the card, reuses a fresh per-card result for 45 seconds, and joins duplicate channel requests without weakening identity checks.

## 5.16.1 — exact messenger-to-deal integrity

- An incoming profile can no longer use an amoCRM conversation context that contradicts the current exact VK, Telegram Personal, or SaleBot link for the deal.
- Exact link replacement removes conflicting conversation contexts in the same transaction, and late stale context writes are rejected.
- amoCRM task delivery rechecks the employee's selected sources, so a queued task is cancelled when its channel was disabled before delivery.

## 5.16.0 — amoCRM tasks by employee and channel

- Each employee can independently choose which incoming channels create amoCRM tasks: MAX, VK, Telegram Personal, and SaleBot.
- Existing employees keep all channels enabled until an administrator changes the selection.
- The master employee switch still disables all amoCRM task creation without disabling message history or Nexus notifications.
- Saving shows a busy spinner and rejects an enabled configuration with no selected channels.

## 5.15.6 — attachment history integrity

- TG Personal keeps the public image URL and media type after sending, and later Telegram history refreshes cannot erase them.
- Queued and duplicate delivery responses include attachment metadata so the widget can paint the image before provider synchronization finishes.
- Failed MAX/Wazzup image attempts remain visible in the dialog; existing failed rows recover the attachment from the durable outbound job without a database rewrite.
- SaleBot attachment types are normalized to its documented `image`, `video`, `audio`, or `file` values; image-only sends include a non-printing caption to prevent its Telegram transport from degrading the media to a plain URL.
- Conversation repaint signatures include attachment metadata, so a background refresh can replace a placeholder with the actual image immediately.

## 5.15.5 — resilient VK image upload

- VK image delivery validates the upload-server response before saving the photo.
- A transient 5xx or incomplete upload response gets one immediate retry with a fresh upload URL before the durable queue uses its normal backoff.
- The VK community ID is passed explicitly when requesting the message-photo upload server.

## 5.15.4 — reliable image delivery and compact profile refresh

- Profile enrichment uses one compact spinner beside the already found profile buttons; the duplicate floating refresh message was removed.
- MAX/Wazzup image attachments are delivered through the documented `contentUri` field. Text plus image is split into two idempotent provider messages because mixed content is not accepted on every transport.
- Temporary HTTP 5xx failures from provider upload servers, including VK image upload 502 responses, stay in the durable retry queue instead of becoming final failures.
- Both amoCRM and GetCourse composers allow images for Wazzup/MAX in addition to VK, TG Personal and SaleBot.

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
