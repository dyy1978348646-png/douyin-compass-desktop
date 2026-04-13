import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config_manager


class LoadConfigTests(unittest.TestCase):
    def test_legacy_custom_date_config_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "config.json"
            config_file.write_text(
                json.dumps(
                    {
                        "date_mode": "custom_date",
                        "custom_date_text": "2026-04-07",
                        "scene_id": "live_review",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.object(config_manager, "CONFIG_FILE", config_file):
                loaded = config_manager.load_config()

        self.assertEqual(loaded["date_mode"], "last_7_days")
        self.assertNotIn("custom_date_text", loaded)
        self.assertEqual(loaded["scene_id"], "live_review")


if __name__ == "__main__":
    unittest.main()
