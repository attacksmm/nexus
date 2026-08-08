import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import aiosqlite
from starlette.requests import Request

import router


def json_request(data):
    body = json.dumps(data).encode()
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({
        "type": "http", "http_version": "1.1", "method": "POST", "scheme": "https",
        "path": "/telegram-attribution", "raw_path": b"/telegram-attribution", "query_string": b"",
        "headers": [(b"content-type", b"application/json")], "server": ("test", 443),
        "client": ("127.0.0.1", 1),
    }, receive)


def form_request(data):
    body = urlencode(data).encode()
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({
        "type": "http", "http_version": "1.1", "method": "POST", "scheme": "https",
        "path": "/telegram-attribution", "raw_path": b"/telegram-attribution", "query_string": b"",
        "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
        "server": ("test", 443), "client": ("127.0.0.1", 1),
    }, receive)


class TelegramAttributionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="senler-attribution-")
        router._db_path = Path(self.tmp.name) / "senler.db"
        router._logger = logging.getLogger("senler-attribution-tests")
        await router._init_db()
        async with aiosqlite.connect(router._db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
                (router.CHANNELS_SETTING_KEY, json.dumps([
                    {"id": "1101081", "name": "Telegram", "api_key": "test-token"}
                ])),
            )
            await db.commit()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_full_utm_is_stored_and_claimed_with_client_id(self):
        created = await router.create_telegram_attribution(json_request({
            "channel_id": "1101081",
            "subscription_id": "3728286",
            "utm_source": "yandex",
            "utm_medium": "cpc",
            "utm_campaign": "very-long-campaign-name",
            "utm_content": "banner-17",
            "utm_term": "исходный длинный поисковый запрос",
            "ym_client_id": "12345678901234567890",
            "yclid": "1234567890",
            "landing_url": "https://example.test/page?utm_term=long",
            "url_params": [["utm_term", "long"], ["custom", "value"]],
        }))
        body = json.loads(created.body)
        self.assertTrue(body["ok"])
        self.assertLessEqual(len("s=3728286-utm_term=12345678901234567890"), 64)

        original = router._apply_telegram_attribution

        async def apply(item):
            self.assertEqual(item["utm_source"], "yandex")
            self.assertEqual(item["utm_campaign"], "very-long-campaign-name")
            self.assertEqual(item["utm_term"], "исходный длинный поисковый запрос")
            self.assertEqual(item["ym_client_id"], "12345678901234567890")
            self.assertEqual(item["tg_user_id"], "777")
            return True, ""

        router._apply_telegram_attribution = apply
        try:
            claimed = await router.claim_telegram_attribution(json_request({
                "client_id": "12345678901234567890", "tg_user_id": "777",
                "subscription_id": "3728286"
            }))
        finally:
            router._apply_telegram_attribution = original
        self.assertTrue(claimed["ok"])
        async with aiosqlite.connect(router._db_path) as db:
            row = await (await db.execute(
                "SELECT status,tg_user_id,utm_source,utm_medium,utm_campaign,utm_content,utm_term,ym_client_id "
                "FROM telegram_attributions WHERE token=?", (body["token"],)
            )).fetchone()
        self.assertEqual(row, (
            "applied", "777", "yandex", "cpc", "very-long-campaign-name", "banner-17",
            "исходный длинный поисковый запрос", "12345678901234567890",
        ))

        with patch.dict(os.environ, {"YANDEX_METRIKA_MEASUREMENT_TOKEN": ""}):
            duplicate = await router.claim_telegram_attribution(json_request({
                "token": body["token"], "tg_user_id": "777", "subscription_id": "3728286"
            }))
        self.assertTrue(duplicate["duplicate"])

        with self.assertRaisesRegex(Exception, "attribution already claimed"):
            await router.claim_telegram_attribution(json_request({
                "token": body["token"], "tg_user_id": "778", "subscription_id": "3728286"
            }))

    async def test_direct_telegram_entry_without_saved_site_row_is_ignored(self):
        result = await router.claim_telegram_attribution(json_request({
            "client_id": "99999999999999999999",
            "tg_user_id": "777",
            "subscription_id": "3728286",
        }))
        self.assertEqual(result["status"], "unmatched")

    async def test_start_before_attribution_is_applied_when_page_data_arrives(self):
        token = "AbCdEf0123456789"
        pending = await router.claim_telegram_attribution(json_request({
            "token": token, "tg_user_id": "777", "subscription_id": "3728286",
        }))
        self.assertEqual(pending["status"], "pending")

        original = router._apply_telegram_attribution

        async def apply(item):
            self.assertEqual(item["token"], token)
            self.assertEqual(item["tg_user_id"], "777")
            self.assertEqual(item["utm_source"], "yandex")
            return True, ""

        router._apply_telegram_attribution = apply
        try:
            created = await router.create_telegram_attribution(json_request({
                "token": token,
                "channel_id": "1101081",
                "subscription_id": "3728286",
                "utm_source": "yandex",
            }))
        finally:
            router._apply_telegram_attribution = original
        self.assertEqual(json.loads(created.body)["claim"], "applied")
        async with aiosqlite.connect(router._db_path) as db:
            row = await (await db.execute(
                "SELECT status,tg_user_id FROM telegram_attributions WHERE token=?", (token,)
            )).fetchone()
            claim = await (await db.execute(
                "SELECT status FROM telegram_attribution_claims WHERE token=?", (token,)
            )).fetchone()
        self.assertEqual(row, ("applied", "777"))
        self.assertEqual(claim, ("applied",))

    async def test_tilda_form_payload_uses_client_token_and_preserves_params(self):
        token = "TildaFormToken12"
        created = await router.create_telegram_attribution(form_request({
            "token": token,
            "channel_id": "1101081",
            "subscription_id": "3728286",
            "utm_source": "tilda",
            "utm_term": "длинный запрос",
            "url_params": json.dumps([["custom", "значение"]], ensure_ascii=False),
        }))
        self.assertTrue(json.loads(created.body)["ok"])
        async with aiosqlite.connect(router._db_path) as db:
            row = await (await db.execute(
                "SELECT utm_source,utm_term,url_params FROM telegram_attributions WHERE token=?", (token,)
            )).fetchone()
        self.assertEqual(row, ("tilda", "длинный запрос", '[["custom", "значение"]]'))

    async def test_senler_custom_variables_use_nexus_names(self):
        calls = []

        class Response:
            is_success = True
            status_code = 200
            text = "ok"

            @staticmethod
            def json():
                return {"success": True}

        class Client:
            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url, data):
                calls.append((url, dict(data)))
                return Response()

        item = {
            "token": "NexusVariables12", "channel_id": "1101081", "subscription_id": "3728286",
            "tg_user_id": "777", "utm_source": "yandex", "utm_medium": "cpc",
            "utm_campaign": "campaign", "utm_content": "banner", "utm_term": "dog training",
            "ym_client_id": "12345678901234567890", "yclid": "click-1",
            "landing_url": "https://example.test/page", "referrer": "https://yandex.ru/",
            "url_params": '[["utm_source","yandex"]]',
        }
        with patch.object(router.httpx, "AsyncClient", Client), patch.dict(
            os.environ, {"YANDEX_METRIKA_MEASUREMENT_TOKEN": ""}
        ):
            self.assertEqual(await router._apply_telegram_attribution(item), (True, ""))
        variables = {data["name"]: data["value"] for url, data in calls if url == router._EP_VAR_SET}
        self.assertEqual(variables["nexus.source"], "yandex")
        self.assertEqual(variables["nexus.medium"], "cpc")
        self.assertEqual(variables["nexus.campaign"], "campaign")
        self.assertEqual(variables["nexus.content"], "banner")
        self.assertEqual(variables["nexus.term"], "dog training")
        self.assertEqual(variables["nexus.ym_uid"], "12345678901234567890")
        self.assertEqual(variables["nexus.yclid"], "click-1")

    async def test_metrika_subscribe_goal_is_sent_once_per_telegram_user(self):
        calls = []

        class Response:
            is_success = True
            status_code = 200
            text = "ok"

        class Client:
            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url, data):
                calls.append((url, dict(data)))
                return Response()

        item = {
            "token": "MetrikaGoalToken", "tg_user_id": "777",
            "ym_client_id": "12345678901234567890",
            "landing_url": "https://example.test/page?utm_source=yandex",
        }
        env = {
            "YANDEX_METRIKA_COUNTER_ID": "96682515",
            "YANDEX_METRIKA_SUBSCRIBE_GOAL": "subscribe",
            "YANDEX_METRIKA_MEASUREMENT_TOKEN": "measurement-secret",
        }
        with patch.object(router.httpx, "AsyncClient", Client), patch.dict(os.environ, env):
            self.assertEqual(await router._send_metrika_subscribe_goal(item), ("sent", ""))
            self.assertEqual(await router._send_metrika_subscribe_goal(item), ("sent", ""))
        self.assertEqual(len(calls), 1)
        url, data = calls[0]
        self.assertEqual(url, router.METRIKA_COLLECT_URL)
        self.assertEqual(data["tid"], "96682515")
        self.assertEqual(data["cid"], "12345678901234567890")
        self.assertEqual(data["t"], "event")
        self.assertEqual(data["ea"], "subscribe")
        self.assertEqual(data["dl"], item["landing_url"])
        self.assertEqual(data["ms"], "measurement-secret")
        async with aiosqlite.connect(router._db_path) as db:
            row = await (await db.execute(
                "SELECT key,value FROM settings WHERE key LIKE 'metrika_goal:%'"
            )).fetchone()
        self.assertEqual(row[0], "metrika_goal:96682515:subscribe:777")
        self.assertEqual(json.loads(row[1])["status"], "sent")
        self.assertEqual(json.loads(row[1])["ym_client_id"], "12345678901234567890")

    async def test_metrika_ambiguous_failure_is_not_retried(self):
        calls = []

        class Client:
            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url, data):
                calls.append((url, dict(data)))
                raise TimeoutError("ambiguous timeout")

        item = {
            "token": "MetrikaFailToken", "tg_user_id": "778",
            "ym_client_id": "22345678901234567890", "landing_url": "https://example.test/",
        }
        with patch.object(router.httpx, "AsyncClient", Client), patch.dict(
            os.environ, {"YANDEX_METRIKA_MEASUREMENT_TOKEN": "measurement-secret"}
        ):
            first = await router._send_metrika_subscribe_goal(item)
            second = await router._send_metrika_subscribe_goal(item)
        self.assertEqual(first[0], "failed")
        self.assertEqual(second[0], "failed")
        self.assertEqual(len(calls), 1)

    async def test_metrika_goal_requires_yandex_client_id(self):
        with patch.dict(os.environ, {"YANDEX_METRIKA_MEASUREMENT_TOKEN": "measurement-secret"}):
            self.assertEqual(await router._send_metrika_subscribe_goal({
                "token": "NoClientIdToken", "tg_user_id": "779", "ym_client_id": "",
            }), ("skipped", ""))

    def test_reconcile_reads_short_token_from_senler_utm(self):
        self.assertEqual(
            router._event_attribution({"utm_term": "n_AbCdEf0123456789"}),
            {"token": "AbCdEf0123456789"},
        )

    async def test_reconcile_matches_senler_registration_to_saved_client_id(self):
        await router.create_telegram_attribution(json_request({
            "channel_id": "1101081",
            "subscription_id": "3728286",
            "utm_source": "yandex",
            "utm_medium": "cpc",
            "utm_campaign": "campaign",
            "utm_content": "banner",
            "utm_term": "original-term",
            "ym_client_id": "12345678901234567890",
        }))
        original_events = router._senler_subscription_events
        original_apply = router._apply_telegram_attribution

        async def events(*_args):
            return [{
                "tg_user_id": 777,
                "subscription_id": 3728286,
                "utm_term": "12345678901234567890",
            }]

        async def apply(item):
            self.assertEqual(item["tg_user_id"], "777")
            return True, ""

        router._senler_subscription_events = events
        router._apply_telegram_attribution = apply
        try:
            self.assertEqual(await router._reconcile_pending_attributions(), 1)
        finally:
            router._senler_subscription_events = original_events
            router._apply_telegram_attribution = original_apply

        async with aiosqlite.connect(router._db_path) as db:
            row = await (await db.execute(
                "SELECT status,tg_user_id FROM telegram_attributions"
            )).fetchone()
        self.assertEqual(row, ("applied", "777"))


if __name__ == "__main__":
    unittest.main()
