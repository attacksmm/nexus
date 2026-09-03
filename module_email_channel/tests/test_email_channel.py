import asyncio
import hashlib
import hmac
import importlib.util
import json
import sys
from email.header import decode_header, make_header
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("_nexus_mod_email-channel", ROOT / "router.py")
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class Lifecycle:
    def create_task(self, coro, name=""):
        coro.close()


@pytest.fixture
def ready(tmp_path, monkeypatch):
    asyncio.run(mod.setup(SimpleNamespace(db_path=tmp_path / "email.db", logger=None, lifecycle=Lifecycle())))
    monkeypatch.setenv(mod.API_KEY_ENV, "test-api")
    monkeypatch.setenv(mod.EVENT_KEY_ENV, "event-secret")
    monkeypatch.setenv(mod.ROUTER_KEY_ENV, "router-secret")
    async def enable():
        db = await mod._connect()
        await db.execute("UPDATE settings SET value='1' WHERE key='enabled'")
        await db.commit(); await db.close()
    asyncio.run(enable())
    return mod


async def _confirmed_send(ready, **kwargs):
    kwargs.setdefault("email_guidelines_confirmed", True)
    kwargs.setdefault("email_guidelines_version", ready.EMAIL_GUIDELINES_VERSION)
    return await ready.service_send(**kwargs)


def test_first_message_requires_subject_and_is_idempotent(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"42","email":"person@example.com","name":"Иван"}
        with pytest.raises(ValueError, match="тему"):
            await _confirmed_send(ready, context=context, text="Привет", idempotency_key="one")
        first = await _confirmed_send(ready, context=context, text="Привет", subject="Ваша заявка", idempotency_key="two", manager_name="Анна")
        again = await _confirmed_send(ready, context=context, text="Другой текст", subject="Другая", idempotency_key="two", manager_name="Анна")
        assert first["thread_id"] == again["public_token"]
        db = await ready._connect()
        assert (await (await db.execute("SELECT COUNT(*) FROM outbound_jobs")).fetchone())[0] == 1
        await db.close()
    asyncio.run(run())


def test_reading_same_email_from_another_card_does_not_mutate_authority(ready):
    async def run():
        a = {"platform":"amocrm","entity_type":"lead","entity_id":"A","email":"shared@example.com"}
        b = {"platform":"amocrm","entity_type":"lead","entity_id":"B","email":"shared@example.com"}
        sent = await _confirmed_send(ready, context=a, text="A", subject="A", idempotency_key="a")
        view = await ready.service_conversation(context=b)
        assert view["thread_id"] == sent["thread_id"]
        db = await ready._connect()
        links = await (await db.execute("SELECT entity_id FROM email_thread_links ORDER BY entity_id")).fetchall()
        await db.close()
        assert [row[0] for row in links] == ["A"]
    asyncio.run(run())


def test_shared_email_cannot_attach_one_thread_to_two_amocrm_leads(ready):
    async def run():
        a = {"platform":"amocrm","entity_type":"lead","entity_id":"A","email":"shared-send@example.com"}
        b = {"platform":"amocrm","entity_type":"lead","entity_id":"B","email":"shared-send@example.com"}
        await _confirmed_send(ready, context=a, text="A", subject="A", idempotency_key="shared-a")
        with pytest.raises(ValueError, match="несколькими переписками"):
            await _confirmed_send(ready, context=b, text="B", subject="B", idempotency_key="shared-b")
        db = await ready._connect()
        links = await (await db.execute("SELECT entity_id FROM email_thread_links")).fetchall()
        await db.close()
        assert [row[0] for row in links] == ["A"]
    asyncio.run(run())


def test_router_reply_token_is_exact_and_deduplicated(ready):
    async def run():
        context = {"platform":"getcourse","entity_type":"user","entity_id":"7","email":"reply@example.com"}
        queued = await _confirmed_send(ready, context=context, text="Тест", subject="Тема", idempotency_key="r")
        timestamp, token = str(int(ready._now().timestamp())), "nonce"
        fields = {"timestamp":timestamp,"token":token,
                  "signature":hmac.new(b"router-secret",(timestamp+token).encode(),hashlib.sha256).hexdigest(),
                  "recipient":f"case+{queued['thread_id']}@support.sobakovod.pro","sender":"reply@example.com",
                  "Message-Id":"<reply-1@example.com>","subject":"Re: Тема","stripped-text":"Ответ"}
        inserted, status, message_id = await ready._store_inbound(fields)
        assert inserted and status == "matched" and message_id
        retry = dict(fields)
        retry.update(timestamp=str(int(ready._now().timestamp()) + 1), token="fresh-nonce")
        retry["signature"] = hmac.new(b"router-secret", (retry["timestamp"] + retry["token"]).encode(), hashlib.sha256).hexdigest()
        inserted2, _, _ = await ready._store_inbound(retry)
        assert not inserted2
    asyncio.run(run())


def test_router_accepts_current_json_webhook_format(ready):
    async def run():
        timestamp, token = str(int(ready._now().timestamp())), "json-nonce"
        payload = {
            "timestamp": timestamp, "token": token,
            "signature": hmac.new(b"router-secret", (timestamp + token).encode(), hashlib.sha256).hexdigest(),
            "recipient": "case+unknown@support.sobakovod.pro", "sender": "client@example.com",
            "subject": "Ответ", "body-plain": "Текст ответа",
            "message-headers": [["Message-ID", "<json-reply@example.com>"], ["Auto-Submitted", "no"]],
            "attachments": [],
        }
        body = json.dumps(payload).encode()
        sent = False
        async def receive():
            nonlocal sent
            if sent:
                return {"type":"http.request", "body":b"", "more_body":False}
            sent = True
            return {"type":"http.request", "body":body, "more_body":False}
        request = Request({"type":"http", "method":"POST", "path":"/", "headers":[
            (b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]}, receive)
        response = await ready.dashamail_inbound(request)
        assert response.status_code == 200
        assert json.loads(response.body)["status"] == "unmatched"
        db = await ready._connect()
        row = await (await db.execute("SELECT rfc_message_id FROM inbound_events")).fetchone()
        await db.close()
        assert row[0] == "<json-reply@example.com>"
    asyncio.run(run())


def test_delivery_webhook_uses_documented_secret_field(ready):
    async def run():
        queued = await _confirmed_send(ready,
            context={"platform":"pilot","entity_type":"test","entity_id":"delivery","email":"delivery@example.com"},
            text="Тест", subject="Тема", idempotency_key="delivery-webhook",
        )
        payload = {
            "event": "delivered",
            "email": "delivery@example.com",
            "message_id": queued["nexus_message_id"],
        }
        payload["secret"] = hashlib.md5(
            (payload["email"] + payload["message_id"] + "event-secret").encode()
        ).hexdigest()
        body = urlencode(payload).encode()
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type":"http.request", "body":b"", "more_body":False}
            sent = True
            return {"type":"http.request", "body":body, "more_body":False}

        request = Request({"type":"http", "method":"POST", "path":"/", "headers":[
            (b"content-type", b"application/x-www-form-urlencoded"),
            (b"content-length", str(len(body)).encode())]}, receive)
        response = await ready.dashamail_events(request)
        assert response.status_code == 200

        invalid = dict(payload)
        invalid["signature"] = invalid.pop("secret")
        invalid_body = urlencode(invalid).encode()
        sent = False

        async def receive_invalid():
            nonlocal sent
            if sent:
                return {"type":"http.request", "body":b"", "more_body":False}
            sent = True
            return {"type":"http.request", "body":invalid_body, "more_body":False}

        invalid_request = Request({"type":"http", "method":"POST", "path":"/", "headers":[
            (b"content-type", b"application/x-www-form-urlencoded"),
            (b"content-length", str(len(invalid_body)).encode())]}, receive_invalid)
        assert (await ready.dashamail_events(invalid_request)).status_code == 401

    asyncio.run(run())


def test_settings_endpoint_uses_current_rate_limit_contract(ready, monkeypatch):
    async def run():
        calls = []

        async def require_admin(_request):
            return {"username": "admin"}

        def rate_limit(request, scope, **kwargs):
            calls.append((request, scope, kwargs))

        monkeypatch.setattr(ready, "_require_admin", require_admin)
        monkeypatch.setattr(ready, "enforce_rate_limit", rate_limit)
        body = json.dumps({"enabled": True, "pilot_mode": True, "inbound_task_mode": "shadow"}).encode()
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type":"http.request", "body":b"", "more_body":False}
            sent = True
            return {"type":"http.request", "body":body, "more_body":False}

        request = Request({"type":"http", "method":"PUT", "path":"/", "headers":[
            (b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]}, receive)
        result = await ready.put_settings(request)
        assert result["settings"]["enabled"] == "1"
        assert calls[0][1:] == (
            "email-channel-settings",
            {"limit": 60, "window_seconds": 3600, "subject": "admin"},
        )

    asyncio.run(run())


def test_bounce_suppresses_future_manual_send(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"9","email":"bounce@example.com"}
        queued = await _confirmed_send(ready, context=context, text="Тест", subject="Тема", idempotency_key="p")
        assert await ready._store_provider_event({"event":"delivered","message_id":queued["nexus_message_id"],"email":"bounce@example.com"})
        assert await ready._store_provider_event({"event":"bounced","message_id":queued["nexus_message_id"],"email":"bounce@example.com","reason":"hard"})
        with pytest.raises(ValueError, match="запрещена"):
            await _confirmed_send(ready, context=context, text="Ещё", idempotency_key="p2")
    asyncio.run(run())


def test_provider_ip_reputation_bounce_does_not_suppress_recipient(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"ip-block","email":"valid@hotmail.com"}
        queued = await _confirmed_send(
            ready, context=context, text="Здравствуйте", subject="Ваша заявка", idempotency_key="ip-block",
        )
        reason = "5.7.1 Service unavailable, Client host [194.104.225.188] blocked using Spamhaus"
        assert await ready._store_provider_event({
            "event":"bounced", "message_id":queued["nexus_message_id"],
            "email":"valid@hotmail.com", "reason":reason,
        })
        db = await ready._connect()
        message = await (await db.execute(
            "SELECT status,error FROM email_messages WHERE nexus_message_id=?",
            (queued["nexus_message_id"],),
        )).fetchone()
        suppression = await (await db.execute(
            "SELECT reason FROM suppressions WHERE email='valid@hotmail.com' AND active=1",
        )).fetchone()
        await db.close()
        assert message["status"] == "failed"
        assert "Spamhaus" in message["error"]
        assert suppression is None
        retry = await _confirmed_send(
            ready, context=context, text="Повторная попытка", idempotency_key="ip-block-retry",
        )
        assert retry["status"] == "queued"

    asyncio.run(run())


@pytest.mark.parametrize(("event", "fields", "expected"), [
    ("bounced", {"reason":"550 5.7.1 recipient address rejected: access denied"}, ""),
    ("bounced", {"reason":"421 4.2.1 mailbox unavailable, try again"}, ""),
    ("bounced", {"reason":"552 5.2.2 mailbox full"}, ""),
    ("bounced", {"reason":"550 5.1.1 user unknown"}, "hard"),
    ("bounced", {"bounce_category":"hard", "reason":"policy did not disclose details"}, "hard"),
    ("bounced", {"bounce_category":"spam_blocked", "reason":"blocked by policy"}, ""),
    ("unsub", {}, "unsubscribe"),
    ("unsubscribed", {}, "unsubscribe"),
])
def test_provider_suppression_matrix(event, fields, expected):
    assert mod._provider_suppression_reason(event, fields) == expected


def test_complaint_suppresses_without_rewriting_delivered_status(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"complaint","email":"complaint@example.com"}
        queued = await _confirmed_send(
            ready, context=context, text="Здравствуйте", subject="Ваша заявка", idempotency_key="complaint",
        )
        base = {"message_id":queued["nexus_message_id"], "email":"complaint@example.com"}
        assert await ready._store_provider_event({**base, "event":"delivered"})
        assert await ready._store_provider_event({**base, "event":"spam"})
        db = await ready._connect()
        status = await (await db.execute(
            "SELECT status FROM email_messages WHERE nexus_message_id=?", (queued["nexus_message_id"],),
        )).fetchone()
        suppression = await (await db.execute(
            "SELECT reason FROM suppressions WHERE email='complaint@example.com' AND active=1",
        )).fetchone()
        await db.close()
        assert status["status"] == "delivered"
        assert suppression["reason"] == "spam"

    asyncio.run(run())


def test_guidelines_ack_is_versioned_and_fail_closed(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"guard-ack","email":"ack@example.com"}
        with pytest.raises(ready.EmailGuardError) as missing:
            await ready.service_send(
                context=context, text="Здравствуйте", subject="Ваша заявка", idempotency_key="ack-missing",
            )
        payload = missing.value.as_dict()
        assert payload["code"] == "email_guidelines_confirmation_required"
        assert payload["confirmation_required"] is True
        assert payload["checklist_version"] == "2026-09-01"
        assert len(payload["checklist"]) == 3
        with pytest.raises(ready.EmailGuardError) as stale:
            await ready.service_send(
                context=context, text="Здравствуйте", subject="Ваша заявка", idempotency_key="ack-stale",
                email_guidelines_confirmed=True, email_guidelines_version="old",
            )
        assert stale.value.code == "email_guidelines_confirmation_required"

        first = await _confirmed_send(
            ready, context=context, text="Здравствуйте", subject="Ваша заявка",
            idempotency_key="ack-first",
        )
        conversation = await ready.service_conversation(context=context)
        assert conversation["thread_id"] == first["thread_id"]
        assert conversation["email_guidelines_required"] is False
        assert conversation["has_chat"] is True
        assert conversation["confirmed_chat"] is False
        second = await ready.service_send(
            context=context, text="Продолжаю переписку", idempotency_key="ack-second",
        )
        assert second["queued"] is True
        db = await ready._connect()
        await db.execute(
            "UPDATE email_messages SET status='accepted' WHERE nexus_message_id=?", (first["nexus_message_id"],),
        )
        await db.commit()
        await db.close()
        assert (await ready.service_conversation(context=context))["confirmed_chat"] is True
    asyncio.run(run())


def test_link_allowlist_preserves_personal_utm_but_blocks_external_short_and_bare_links(ready):
    async def run():
        base = {"platform":"amocrm","entity_type":"lead","phone":"+7 999 111-22-33","name":"Иван Иванов"}
        allowed = dict(base, entity_id="guard-link-ok", email="allowed@example.com")
        result = await _confirmed_send(
            ready, context=allowed,
            text="https://club.sobakovod.pro/page?utm_term=12345678901&utm_campaign=987654321&param1=123456789012345678",
            subject="Информация", idempotency_key="guard-link-ok",
        )
        assert result["queued"] is True

        cases = [
            ("external", "https://evil.example/path", "email_link_domain_not_allowed"),
            ("short", "https://bit.ly/example", "email_short_link_not_allowed"),
            ("bare", "www.evil.example/path", "email_link_scheme_required"),
            ("nested", "https://sobakovod.pro/go?next=https%3A%2F%2Fevil.example%2Fx", "email_nested_link_not_allowed"),
        ]
        for suffix, text, code in cases:
            with pytest.raises(ready.EmailGuardError) as blocked:
                await _confirmed_send(
                    ready, context=dict(base, entity_id=f"guard-link-{suffix}", email=f"{suffix}@example.com"), text=text,
                    subject="Информация", idempotency_key=f"guard-link-{suffix}",
                )
            assert blocked.value.code == code
    asyncio.run(run())


def test_urls_cannot_contain_contact_or_generic_personal_data(ready):
    async def run():
        base = {
            "platform":"amocrm", "entity_type":"lead", "email":"client@example.com",
            "phone":"+7 999 111-22-33", "name":"Иван Иванов",
        }
        cases = [
            ("email", "https://sobakovod.pro/page?client=client%40example.com", "email"),
            ("phone", "https://sobakovod.pro/page?client=79991112233", "phone"),
            ("generic-phone", "https://sobakovod.pro/page?mobile=79001234567", "phone"),
            ("name", "https://sobakovod.pro/client/%D0%98%D0%B2%D0%B0%D0%BD-%D0%98%D0%B2%D0%B0%D0%BD%D0%BE%D0%B2", "name"),
        ]
        for suffix, text, kind in cases:
            with pytest.raises(ready.EmailGuardError) as blocked:
                await _confirmed_send(
                    ready, context=dict(base, entity_id=f"guard-pii-{suffix}"), text=text,
                    subject="Информация", idempotency_key=f"guard-pii-{suffix}",
                )
            assert blocked.value.code == "email_url_contains_personal_data"
            assert blocked.value.details["personal_data"] == kind
    asyncio.run(run())


def test_first_outgoing_rejects_multiple_links_and_any_attachment(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"guard-first","email":"first@example.com"}
        with pytest.raises(ready.EmailGuardError) as links:
            await _confirmed_send(
                ready, context=context,
                text="https://sobakovod.pro/one https://club.sobakovod.pro/two",
                subject="Информация", idempotency_key="guard-first-links",
            )
        assert links.value.code == "email_first_message_too_many_links"
        # A merely queued/failed attempt does not establish correspondence and
        # must not lift first-message protections.
        await _confirmed_send(
            ready, context=context, text="https://sobakovod.pro/one",
            subject="Информация", idempotency_key="guard-first-queued",
        )
        with pytest.raises(ready.EmailGuardError) as queued_links:
            await _confirmed_send(
                ready, context=context,
                text="https://sobakovod.pro/one https://club.sobakovod.pro/two",
                subject="Информация", idempotency_key="guard-first-queued-links",
            )
        assert queued_links.value.code == "email_first_message_too_many_links"
        with pytest.raises(ready.EmailGuardError) as attachment:
            await _confirmed_send(
                ready, context=context, text="Здравствуйте", subject="Информация",
                idempotency_key="guard-first-attachment",
                attachment_url="https://junior.sobakovod.pro/media/file.zip", attachment_type="application/zip",
            )
        assert attachment.value.code == "email_first_message_attachment_not_allowed"
    asyncio.run(run())


def test_unsubscribe_after_claim_cancels_job_before_provider_submit(ready, monkeypatch):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"guard-race","email":"race@example.com"}
        await _confirmed_send(
            ready, context=context, text="Здравствуйте", subject="Информация", idempotency_key="guard-race",
        )
        job = await ready._claim_job()
        assert job and job["status"] == "pending"
        await ready._suppress_unsubscribed("race@example.com")
        called = False

        class NeverClient:
            def __init__(self, **_kwargs):
                nonlocal called
                called = True

        monkeypatch.setattr(ready.httpx, "AsyncClient", NeverClient)
        await ready._process_job(job)
        assert called is False
        db = await ready._connect()
        row = await (await db.execute(
            """SELECT j.status,m.status,m.error FROM outbound_jobs j
               JOIN email_messages m ON m.id=j.message_id WHERE j.id=?""", (job["id"],),
        )).fetchone()
        await db.close()
        assert tuple(row[:2]) == ("failed", "failed")
        assert "отписался" in row["error"]
    asyncio.run(run())


def test_bounce_without_email_resolves_recipient_and_cancels_queue(ready):
    async def run():
        queued = await _confirmed_send(
            ready,
            context={"platform":"amocrm","entity_type":"lead","entity_id":"guard-no-email","email":"no-email@example.com"},
            text="Здравствуйте", subject="Информация", idempotency_key="guard-no-email",
        )
        assert await ready._store_provider_event({
            "event":"bounced", "message_id":queued["nexus_message_id"], "reason":"hard",
        })
        db = await ready._connect()
        suppression = await (await db.execute(
            "SELECT reason FROM suppressions WHERE email='no-email@example.com' AND active=1",
        )).fetchone()
        job = await (await db.execute(
            "SELECT status FROM outbound_jobs WHERE idempotency_key='guard-no-email'",
        )).fetchone()
        await db.close()
        assert suppression["reason"] == "hard"
        assert job["status"] == "failed"
    asyncio.run(run())


def test_delivery_status_never_moves_backwards(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"10","email":"status@example.com"}
        queued = await _confirmed_send(ready, context=context, text="Тест", subject="Тема", idempotency_key="status")
        base = {"message_id":queued["nexus_message_id"], "email":"status@example.com", "transaction_id":"tx-1"}
        assert await ready._store_provider_event({**base, "event":"delivered"})
        assert await ready._store_provider_event({**base, "event":"sent"})
        db = await ready._connect()
        status = (await (await db.execute("SELECT status FROM email_messages WHERE nexus_message_id=?", (queued["nexus_message_id"],))).fetchone())[0]
        await db.close()
        assert status == "delivered"
    asyncio.run(run())


def _request(method="GET", query="", body=b""):
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type":"http.request", "body":b"", "more_body":False}
        sent = True
        return {"type":"http.request", "body":body, "more_body":False}

    return Request({
        "type":"http", "method":method, "path":"/", "query_string":query.encode(),
        "client":("127.0.0.1", 12345),
        "headers":[(b"content-length", str(len(body)).encode())],
    }, receive)


def test_sender_profile_is_stable_and_uses_personal_alias(ready):
    async def run():
        context = {"platform":"amocrm","entity_type":"lead","entity_id":"sender-a","email":"sender-a@example.com"}
        first = await _confirmed_send(ready,
            context=context, text="Привет", subject="Тема", idempotency_key="sender-a",
            manager_id="17", manager_name="Татьяна Истратова", from_name="Татьяна Истратова",
        )
        assert first["from_email"] == "titiana.i@support.sobakovod.pro"
        db = await ready._connect()
        profile = await (await db.execute("SELECT * FROM sender_profiles WHERE manager_id='17'")).fetchone()
        assert profile["local_part"] == "titiana.i"
        await db.close()
        second = await _confirmed_send(ready,
            context=context, text="Ещё", idempotency_key="sender-b",
            manager_id="17", manager_name="Татьяна Истратова", from_name="Татьяна Истратова",
        )
        assert second["from_email"] == "titiana.i@support.sobakovod.pro"
        collision = await ready._ensure_sender_profile("18", "Татьяна Истратова")
        assert collision["local_part"] == "titiana.i-2"
    asyncio.run(run())


def test_short_sender_aliases_and_legacy_profile_migration(ready):
    async def run():
        assert ready._suggest_local_part("Никита Попов", "1") == "nikita.p"
        assert ready._suggest_local_part("Татьяна Воробьева", "2") == "tatiana.v"
        db = await ready._connect()
        now = ready._iso()
        await db.execute(
            "INSERT INTO sender_profiles(manager_id,manager_name,local_part,created_at,updated_at) VALUES(?,?,?,?,?)",
            ("2", "Татьяна Воробьева", "tatyana.vorobeva", now, now),
        )
        await db.execute(
            "INSERT INTO sender_profiles(manager_id,manager_name,local_part,created_at,updated_at) VALUES(?,?,?,?,?)",
            ("3", "Никита Попов", "my-custom-address", now, now),
        )
        await db.commit()
        await db.close()
        migrated = await ready._ensure_sender_profile("2", "Татьяна Воробьева")
        custom = await ready._ensure_sender_profile("3", "Никита Попов")
        assert migrated["local_part"] == "tatiana.v"
        assert custom["local_part"] == "my-custom-address"
    asyncio.run(run())


def test_branded_html_escapes_content_and_keeps_real_links(ready):
    unsubscribe = ready._visible_unsubscribe_url("person@example.com")
    rendered = ready._render_email_html(
        'Здравствуйте <script>alert(1)</script>\nhttps://sobakovod.pro/course?a=1&b=2',
        'Татьяна Истратова <img src=x>', unsubscribe,
    )
    plain = ready._render_plain_text("Здравствуйте", "Татьяна Истратова", unsubscribe)
    assert "Современный собаковод" in rendered
    assert "#5aacec" in rendered
    assert "<script>" not in rendered and "&lt;script&gt;" in rendered
    assert "<img src=x>" not in rendered and "&lt;img src=x&gt;" in rendered
    assert 'href="https://sobakovod.pro/course?a=1&amp;b=2"' in rendered
    assert "Татьяна Истратова" in plain and "Отписаться от писем:" in plain


def test_personal_attribution_link_is_persisted_in_branded_message(ready):
    async def run():
        context = {
            "platform":"amocrm", "entity_type":"lead", "entity_id":"utm-signature",
            "email":"signature@example.com",
        }
        signature_url = (
            "https://sobakovod.pro/?utm_term=741919467&utm_source=vk_ai&utm_medium=cpc"
            "&utm_campaign=1030134194&utm_content=177638832&param1=&param2=17696535"
        )
        await _confirmed_send(
            ready, context=context, text="Здравствуйте", subject="Ваша заявка",
            idempotency_key="signature-personal", manager_name="Никита Попов",
            signature_url=signature_url,
        )
        db = await ready._connect()
        row = await (await db.execute(
            "SELECT html_body FROM email_messages WHERE direction='outgoing'",
        )).fetchone()
        await db.close()
        assert f'href="{signature_url.replace("&", "&amp;")}"' in row[0]
        assert ready._signature_url_from_html(row[0]) == signature_url

        default_context = dict(context, entity_id="utm-signature-default", email="default@example.com")
        result = await _confirmed_send(
            ready, context=default_context, text="Здравствуйте", subject="Ваша заявка",
            idempotency_key="signature-default", signature_url="https://sobakovod.pro/",
        )
        assert result["queued"] is True
    asyncio.run(run())


def test_submit_adds_signature_and_one_click_headers(ready, monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        headers = {}
        text = ""
        def json(self):
            return {"response":{"msg":{"err_code":0},"data":{"transaction_id":"tx-1"}}}

    class FakeClient:
        def __init__(self, **_kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return None
        async def post(self, _url, json):
            captured.update(json)
            return FakeResponse()

    monkeypatch.setattr(ready.httpx, "AsyncClient", FakeClient)

    async def run():
        transaction_id, _ = await ready._submit({
            "public_token":"abc", "nexus_message_id":"message-1", "last_rfc_message_id":"",
            "to_email":"person@example.com", "from_email":"tatyana.istratova@support.sobakovod.pro",
            "manager_name":"Татьяна Истратова", "subject":"Тема", "text_body":"Текст",
        })
        assert transaction_id == "tx-1"
        headers = json.loads(captured["headers"])
        assert headers["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
        assert "/unsubscribe/one-click?" in headers["List-Unsubscribe"]
        assert captured["from_name"] == "Татьяна · Современный собаковод"
        assert "Современный собаковод" in str(make_header(decode_header(headers["Reply-To"])))
        assert "case+abc@support.sobakovod.pro" in headers["Reply-To"]
        assert "С заботой о вас" not in captured["message"]
        assert "Отписаться от писем" in captured["message"]
        assert "Отписаться от писем:" in captured["plain_text"]
    asyncio.run(run())


def test_visible_unsubscribe_get_does_not_mutate_and_post_blocks_nexus(ready):
    async def run():
        query = ready._unsubscribe_query("unsubscribe@example.com")
        page = await ready.unsubscribe_confirmation(_request(query=query))
        assert page.status_code == 200 and "Подтвердите отписку" in page.body.decode()
        db = await ready._connect()
        assert (await (await db.execute("SELECT COUNT(*) FROM suppressions")).fetchone())[0] == 0
        await db.close()
        complete = await ready.unsubscribe_submit(_request(method="POST", query=query))
        assert complete.status_code == 200 and "Вы отписаны" in complete.body.decode()
        complete_again = await ready.unsubscribe_submit(_request(method="POST", query=query))
        assert complete_again.status_code == 200
        with pytest.raises(ValueError, match="запрещена"):
            await _confirmed_send(ready,
                context={"platform":"amocrm","entity_type":"lead","entity_id":"unsub","email":"unsubscribe@example.com"},
                text="Тест", subject="Тема", idempotency_key="unsubscribed",
            )
    asyncio.run(run())


def test_one_click_unsubscribe_requires_rfc_post_body(ready):
    async def run():
        query = ready._unsubscribe_query("one-click@example.com")
        with pytest.raises(Exception) as invalid:
            await ready.unsubscribe_one_click(_request(method="POST", query=query, body=b"wrong"))
        assert getattr(invalid.value, "status_code", None) == 400
        response = await ready.unsubscribe_one_click(_request(
            method="POST", query=query, body=b"List-Unsubscribe=One-Click",
        ))
        assert response.status_code == 204
        db = await ready._connect()
        row = await (await db.execute("SELECT reason,active FROM suppressions WHERE email='one-click@example.com'")).fetchone()
        await db.close()
        assert tuple(row) == ("unsubscribe", 1)
    asyncio.run(run())


def test_disabled_sender_uses_fallback_address(ready):
    async def run():
        await ready._ensure_sender_profile("77", "Ирина Демидова")
        db = await ready._connect()
        await db.execute("UPDATE sender_profiles SET enabled=0 WHERE manager_id='77'")
        await db.commit(); await db.close()
        result = await _confirmed_send(ready,
            context={"platform":"amocrm","entity_type":"lead","entity_id":"fallback","email":"fallback@example.com"},
            text="Тест", subject="Тема", idempotency_key="fallback-sender",
            manager_id="77", manager_name="Ирина Демидова",
        )
        assert result["from_email"] == "info@support.sobakovod.pro"
    asyncio.run(run())


def test_panel_keeps_mobile_senders_reachable_and_actions_busy():
    panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
    assert "html{height:auto;min-height:100%;overflow-y:auto}" in panel
    assert "main{overflow:visible}" in panel
    assert "Проверяем настройки и очередь…" in panel
    assert "Загружаем цепочки…" in panel
    assert "busy(b,'Сохраняем…')" in panel
    assert "Проверить DashaMail" not in panel
