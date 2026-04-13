import tempfile
import unittest
from pathlib import Path

from runtime_env import (
    bundled_playwright_browser_root,
    has_browser_install,
    pick_playwright_browser_root,
)

class PlaywrightBrowserPathTests(unittest.TestCase):
    def test_bundled_playwright_browser_root_matches_local_browsers_layout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            browser_root = root / "playwright" / "driver" / "package" / ".local-browsers"
            browser_root.mkdir(parents=True)
            (browser_root / "chromium-1234").mkdir()

            self.assertEqual(bundled_playwright_browser_root(root), browser_root)

    def test_has_browser_install_requires_chromium_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            browser_root = Path(temp_dir)
            self.assertFalse(has_browser_install(browser_root))
            (browser_root / "chromium-1234").mkdir()
            self.assertTrue(has_browser_install(browser_root))

    def test_pick_playwright_browser_root_prefers_bundled_copy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            bundled_root = temp_root / "bundled"
            fallback_root = temp_root / "fallback"
            bundled_root.mkdir()
            fallback_root.mkdir()
            (bundled_root / "chromium-1234").mkdir()

            picked = pick_playwright_browser_root(
                candidates=[bundled_root],
                fallback_root=fallback_root,
            )

            self.assertEqual(picked, bundled_root)

    def test_pick_playwright_browser_root_falls_back_to_writable_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            empty_root = temp_root / "empty"
            fallback_root = temp_root / "fallback"
            empty_root.mkdir()

            picked = pick_playwright_browser_root(
                candidates=[empty_root],
                fallback_root=fallback_root,
            )

            self.assertEqual(picked, fallback_root)
            self.assertTrue(fallback_root.exists())


if __name__ == "__main__":
    unittest.main()
