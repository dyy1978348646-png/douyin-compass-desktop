import unittest
from pathlib import Path

from build_support import (
    dedupe_datas,
    playwright_browser_directory_name,
    playwright_cache_candidates,
    pyinstaller_excludes,
    pyinstaller_hiddenimports,
)


class BuildSupportTests(unittest.TestCase):
    def test_dedupe_datas_removes_duplicate_entries(self):
        entries = [
            ("assets", "assets"),
            ("assets", "assets"),
            ("ms-playwright", "ms-playwright"),
        ]

        self.assertEqual(
            dedupe_datas(entries),
            [("assets", "assets"), ("ms-playwright", "ms-playwright")],
        )

    def test_pyinstaller_excludes_contain_heavy_unused_modules(self):
        excludes = pyinstaller_excludes()
        self.assertIn("pyarrow", excludes)
        self.assertIn("numba", excludes)
        self.assertIn("llvmlite", excludes)
        self.assertNotIn("pandas", excludes)

    def test_hiddenimports_are_minimal_and_platform_aware(self):
        common = pyinstaller_hiddenimports("linux")
        self.assertIn("playwright.sync_api", common)
        self.assertIn("pandas", common)
        self.assertNotIn("rumps", common)

        macos = pyinstaller_hiddenimports("darwin")
        self.assertIn("rumps", macos)

    def test_playwright_browser_directory_name_matches_cache_layout(self):
        self.assertEqual(
            playwright_browser_directory_name("chromium", "1208"),
            "chromium-1208",
        )
        self.assertEqual(
            playwright_browser_directory_name("chromium-headless-shell", "1208"),
            "chromium_headless_shell-1208",
        )

    def test_playwright_cache_candidates_are_platform_aware(self):
        self.assertEqual(
            playwright_cache_candidates("Darwin", home="/Users/tester"),
            [Path("/Users/tester/Library/Caches/ms-playwright")],
        )
        self.assertEqual(
            playwright_cache_candidates(
                "Windows",
                home="C:/Users/tester",
                local_appdata="C:/Users/tester/AppData/Local",
            ),
            [Path("C:/Users/tester/AppData/Local/ms-playwright")],
        )
        self.assertEqual(
            playwright_cache_candidates("linux", home="/home/tester"),
            [Path("/home/tester/.cache/ms-playwright")],
        )


if __name__ == "__main__":
    unittest.main()
