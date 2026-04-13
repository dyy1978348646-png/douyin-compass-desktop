import tempfile
import unittest
from pathlib import Path

from build_support import (
    APP_NAME,
    render_windows_launcher_bat,
    render_windows_quickstart_text,
    windows_release_dir_name,
)


class BuildSupportTests(unittest.TestCase):
    def test_windows_release_dir_name_is_novice_friendly(self):
        self.assertEqual(windows_release_dir_name(APP_NAME), "抖音罗盘抓取器_Windows版")

    def test_launcher_bat_points_to_exe_in_same_folder(self):
        launcher = render_windows_launcher_bat(f"{APP_NAME}.exe")
        self.assertIn(f"start \"\" \"%~dp0{APP_NAME}.exe\"", launcher)
        self.assertIn("cd /d \"%~dp0\"", launcher)

    def test_quickstart_text_mentions_extract_and_double_click(self):
        readme_text = render_windows_quickstart_text(
            app_name=APP_NAME,
            launcher_name=f"双击启动-{APP_NAME}.bat",
        )
        self.assertIn("先完整解压", readme_text)
        self.assertIn(f"双击启动-{APP_NAME}.bat", readme_text)
        self.assertIn("无需安装 Python", readme_text)


if __name__ == "__main__":
    unittest.main()
