import os

import pytest

from orchestrator import telegram_proxy as proxy


def test_canonical_bot_proxy_wins_over_module_legacy(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_API_PROXY_URL", "http://global.example:3128")
    monkeypatch.setenv("SBKVD_LETTER_TELEGRAM_PROXY_URL", "http://legacy.example:3129")
    assert proxy.telegram_bot_api_proxy_url("http://module.example:3130") == "http://global.example:3128"


def test_legacy_bot_proxy_is_fallback(monkeypatch):
    for key in proxy.BOT_API_PROXY_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SBKVD_LETTER_TELEGRAM_PROXY_URL", "http://legacy.example:3129")
    assert proxy.telegram_bot_api_proxy_url() == "http://legacy.example:3129"


def test_mtproto_tme_url_parsing_and_masking():
    value = "https://t.me/proxy?server=138.124.92.192&port=9443&secret=abcdef"
    assert proxy.mtproto_proxy_parts(value) == ("138.124.92.192", 9443, "abcdef")
    assert proxy.masked_proxy(value, kind="mtproto") == "138.124.92.192:9443 · secret ••••"
    assert "abcdef" not in proxy.masked_proxy(value, kind="mtproto")


def test_proxy_validation_rejects_unsafe_or_incomplete_values():
    assert proxy.validate_bot_api_proxy("socks5://127.0.0.1:1080")
    with pytest.raises(ValueError):
        proxy.validate_bot_api_proxy("http://missing-port.example")
    with pytest.raises(ValueError):
        proxy.validate_mtproto_proxy("https://t.me/proxy?server=x&port=443")


def test_explicit_httpx_candidate_is_not_replaced_by_current_global(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_API_PROXY_URL", "http://current.example:3128")
    kwargs = proxy.httpx_client_kwargs(proxy_url="http://candidate.example:3129")
    assert kwargs["proxy"] == "http://candidate.example:3129"


def test_explicit_mtproto_candidate_is_not_replaced_by_current_global(monkeypatch):
    monkeypatch.setenv(
        "TELEGRAM_MTPROTO_PROXY_URL",
        "https://t.me/proxy?server=current.example&port=443&secret=aaaa",
    )
    candidate = "https://t.me/proxy?server=candidate.example&port=9443&secret=bbbb"
    _, config = proxy.telethon_proxy_config(candidate)
    assert config == ("candidate.example", 9443, "bbbb")
