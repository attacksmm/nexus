import re
import sqlite3
from types import SimpleNamespace

from module_chat_moderators import router as module


def test_community_peer_discovery_replaces_personal_side_mapping(tmp_path, monkeypatch):
    database = tmp_path / "chat-moderators.db"
    monkeypatch.setattr(
        module,
        "_ctx",
        SimpleNamespace(db_path=database, data_dir=tmp_path, module_dir=tmp_path),
    )
    module._init_db()
    title = "41. 13.02.2026 - Современный Собаковод - закрытый чат"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """INSERT INTO managed_chats(
                   platform,chat_id,peer_id,title,zone,enabled,last_seen_at,meta_json
               ) VALUES('vk','','2000000051',?,'closed_club',1,'2026-07-03T04:40:31Z','{}')""",
            (title,),
        )
        connection.execute(
            """INSERT INTO managed_chats(
                   platform,chat_id,peer_id,title,zone,enabled,last_seen_at,meta_json
               ) VALUES('vk','','2000000066','88. 01.01.1990 - Курс Щенок. Современный Собаковод',
                        'training_stream',1,'2026-08-08T00:00:00Z','{}')"""
        )

    class FakeVK:
        @staticmethod
        def execute(*, code):
            results = []
            for peer_id in map(int, re.findall(r'"peer_ids":(\d+)', code)):
                if peer_id != 2_000_000_066:
                    results.append(False)
                    continue
                results.append({
                    "items": [{
                        "conversation": {
                            "peer": {"id": peer_id},
                            "chat_settings": {"title": title},
                        }
                    }]
                })
            return results

    runtime = module.VKModeratorRuntime(analyzer=None)
    runtime.vk = FakeVK()
    runtime.settings = {}
    result = runtime._discover_community_chat_peers_sync()

    assert result["discovered"] == 1
    assert result["updated"] == 1
    assert result["stale_disabled"] == 1
    assert result["highest_existing_local_id"] == 66
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT peer_id,title,zone,enabled FROM managed_chats ORDER BY peer_id"
        ).fetchall()
    assert rows == [
        ("2000000051", title, "closed_club", 0),
        ("2000000066", title, "closed_club", 1),
    ]
