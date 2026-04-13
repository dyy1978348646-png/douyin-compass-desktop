import tempfile
import unittest
from pathlib import Path

from scraper import (
    chrome_executable_candidates,
    chrome_user_data_candidates,
    first_existing_path,
)


class BrowserBootstrapTests(unittest.TestCase):
    def test_mac_chrome_executable_candidates(self):
        home = Path("/Users/tester")
        candidates = chrome_executable_candidates(system_name="Darwin", home=home, env={})
        self.assertEqual(
            candidates[0],
            Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        )
        self.assertEqual(
            candidates[1],
            home / "Applications" / "Google Chrome.app" / "Contents" / "MacOS" / "Google Chrome",
        )

    def test_windows_chrome_executable_candidates(self):
        env = {
            "PROGRAMFILES": r"C:\Program Files",
            "PROGRAMFILES(X86)": r"C:\Program Files (x86)",
            "LOCALAPPDATA": r"C:\Users\tester\AppData\Local",
        }
        candidates = chrome_executable_candidates(system_name="Windows", home=Path("C:/Users/tester"), env=env)
        self.assertEqual(
            candidates,
            [
                Path(env["PROGRAMFILES"]) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(env["PROGRAMFILES(X86)"]) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(env["LOCALAPPDATA"]) / "Google" / "Chrome" / "Application" / "chrome.exe",
            ],
        )

    def test_mac_chrome_user_data_candidates(self):
        home = Path("/Users/tester")
        candidates = chrome_user_data_candidates(system_name="Darwin", home=home, env={})
        self.assertEqual(
            candidates,
            [home / "Library" / "Application Support" / "Google" / "Chrome"],
        )

    def test_first_existing_path_returns_first_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "first"
            second = root / "second"
            second.mkdir()
            self.assertEqual(first_existing_path([first, second]), second)


if __name__ == "__main__":
    unittest.main()
