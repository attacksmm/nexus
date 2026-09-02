from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from orchestrator import auth


def test_legacy_user_mutations_are_locked_only_while_registry_is_loaded(monkeypatch):
    monkeypatch.delitem(sys.modules, "_nexus_mod_staff-registry", raising=False)
    auth._reject_legacy_user_mutation()

    monkeypatch.setitem(sys.modules, "_nexus_mod_staff-registry", SimpleNamespace())
    with pytest.raises(auth.HTTPException) as exc:
        auth._reject_legacy_user_mutation()

    assert exc.value.status_code == 409
    assert exc.value.headers["Location"] == "/nexus/staff-registry/panel/"


def test_settings_has_only_central_staff_entry_point():
    html = (Path(__file__).resolve().parents[1] / "templates" / "settings.html").read_text("utf-8")
    users_section = html.split('id="tabUsers"', 1)[1].split("</section>", 1)[0]

    assert "/nexus/staff-registry/panel/" in users_section
    assert 'id="addUserBtn"' not in users_section
    assert 'id="usersTable"' not in users_section
    assert 'id="userDialog"' not in html
