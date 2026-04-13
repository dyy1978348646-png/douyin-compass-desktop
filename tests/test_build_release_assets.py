import tempfile
import unittest
from pathlib import Path

from build_release_assets import prepare_windows_release
from build_support import APP_NAME, windows_release_dir_name


class BuildReleaseAssetsTests(unittest.TestCase):
    def test_prepare_windows_release_copies_app_and_creates_launcher(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            dist_dir = Path(tmp_dir) / "dist"
            source_dir = dist_dir / APP_NAME
            source_dir.mkdir(parents=True)
            (source_dir / f"{APP_NAME}.exe").write_text("exe", encoding="utf-8")
            (source_dir / "_internal.txt").write_text("internal", encoding="utf-8")

            release_dir = prepare_windows_release(dist_dir, APP_NAME)

            self.assertEqual(release_dir.name, windows_release_dir_name(APP_NAME))
            self.assertTrue((release_dir / f"{APP_NAME}.exe").exists())
            self.assertTrue((release_dir / "_internal.txt").exists())
            self.assertTrue((release_dir / f"双击启动-{APP_NAME}.bat").exists())
            self.assertTrue((release_dir / "Windows使用说明.txt").exists())


if __name__ == "__main__":
    unittest.main()
