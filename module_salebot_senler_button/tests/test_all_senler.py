import json
import unittest
from pathlib import Path
from urllib.parse import urlencode

from starlette.requests import Request

import router


ROOT = Path(__file__).resolve().parents[1]


def request_with_query(params):
    query = urlencode(params).encode()
    return Request({
        "type": "http", "http_version": "1.1", "method": "GET", "scheme": "https",
        "path": "/all-senler.js", "raw_path": b"/all-senler.js", "query_string": query,
        "headers": [], "server": ("test", 443), "client": ("127.0.0.1", 1),
    })


class AllSenlerScriptTests(unittest.IsolatedAsyncioTestCase):
    async def test_script_replaces_both_channels_and_uses_client_id_attribution(self):
        response = await router.all_senler_js(request_with_query({
            "telegram_bot": "Master_class_for_dog_owners_bot",
            "telegram_channel_id": "1101081",
            "telegram_subscription_id": "3728286",
            "vk_text": "ВКонтакте",
            "telegram_text": "Telegram",
        }))
        script = response.body.decode()
        for marker in (
            "nexus-all-senler-btn--vk",
            "nexus-all-senler-btn--tg",
            "utm_source:value('utm_source')",
            "utm_medium:value('utm_medium')",
            "utm_campaign:value('utm_campaign')",
            "utm_content:value('utm_content')",
            "utm_term:value('utm_term')",
            "ym_client_id:clientId",
            "yclid:value('yclid')||cookie('yclid')",
            "url_params:params",
            "pageToken=window.__nexusAllSenlerToken||localToken()",
            "token:pageToken",
            "function tokenTelegramHref()",
            "function directTelegramHref(id)",
            "function clientTelegramHref(id)",
            "start.length<=64",
            "if(clientId()!==attributionClientId)prepareAttribution(true)",
            "-utm_term='+id",
            "keepalive:true",
            "-utm_term=n_'+pageToken",
            "setTimeout(function(){resolve(direct||fallback);},1500)",
            router.TELEGRAM_ATTRIBUTION_URL,
        ):
            self.assertIn(marker, script)
        self.assertIn('"telegramSubscriptionId": "3728286"', script)

    async def test_invalid_telegram_values_fall_back_safely(self):
        response = await router.all_senler_js(request_with_query({
            "telegram_bot": "https://evil.example/bot",
            "telegram_channel_id": "not numeric",
            "telegram_subscription_id": "<script>",
        }))
        script = response.body.decode()
        self.assertIn(json.dumps(router.DEFAULT_TELEGRAM_BOT), script)
        self.assertIn(json.dumps(router.DEFAULT_TELEGRAM_CHANNEL_ID), script)
        self.assertIn(json.dumps(router.DEFAULT_TELEGRAM_SUBSCRIPTION_ID), script)

    def test_panel_exposes_all_senler_mode(self):
        panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
        manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["version"], "1.3.2")
        for marker in (
            'data-view="allSenler"', 'id="telegramBot"', 'id="telegramSubscriptionId"',
            "window.__nexusAllSenlerToken=token()", "nexus_senler_queue_v1",
            "keepalive:true", "window.addEventListener('online',flush)",
            "if(!originals.length)return", "host.insertBefore(existing,anchor)",
        ):
            self.assertIn(marker, panel)

    def test_panel_surfaces_api_failures_and_touch_controls(self):
        panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
        for marker in (
            "Загрузка…",
            "Не удалось загрузить:",
            "Сохранение…",
            "Не удалось сохранить:",
            "Удаление…",
            "Не удалось удалить:",
            "Не удалось скопировать:",
            "min-height:40px",
        ):
            self.assertIn(marker, panel)


if __name__ == "__main__":
    unittest.main()
