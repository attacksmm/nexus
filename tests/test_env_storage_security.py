import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator import auth


class EnvStorageSecurityTests(unittest.TestCase):
    def test_write_keeps_single_owner_only_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text("TOKEN=old\n", encoding="utf-8")
            env_path.with_name(".env.bak").write_text("TOKEN=old\n", encoding="utf-8")

            with patch.object(auth, "ENV_PATH", env_path):
                auth._write_env_values({"TOKEN": "new"})

            self.assertEqual(auth._parse_env_content(env_path.read_text()), {"TOKEN": "new"})
            self.assertEqual(stat.S_IMODE(env_path.stat().st_mode), 0o600)
            self.assertFalse(env_path.with_name(".env.bak").exists())

    def test_read_repairs_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text("TOKEN=value\n", encoding="utf-8")
            env_path.chmod(0o644)

            with patch.object(auth, "ENV_PATH", env_path):
                self.assertEqual(auth._read_env_values(), {"TOKEN": "value"})

            self.assertEqual(stat.S_IMODE(env_path.stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
