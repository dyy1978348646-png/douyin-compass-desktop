"""
打包辅助配置，供 PyInstaller spec 和测试共用。
"""

from __future__ import annotations

from pathlib import Path

PLAYWRIGHT_BROWSER_DIR_ALIASES = {
    "chromium-headless-shell": "chromium_headless_shell",
}


def dedupe_datas(entries: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    unique_entries: list[tuple[str, str]] = []

    for entry in entries:
        if entry in seen:
            continue
        seen.add(entry)
        unique_entries.append(entry)

    return unique_entries


def pyinstaller_hiddenimports(platform_name: str) -> list[str]:
    hiddenimports = [
        "playwright",
        "playwright.sync_api",
        "pandas",
        "openpyxl",
        "pystray",
        "PIL",
    ]
    if platform_name == "darwin":
        hiddenimports.append("rumps")
    return hiddenimports


def pyinstaller_excludes() -> list[str]:
    return [
        "pyarrow",
        "numba",
        "llvmlite",
    ]


def playwright_browser_directory_name(name: str, revision: str) -> str:
    normalized_name = PLAYWRIGHT_BROWSER_DIR_ALIASES.get(name, name)
    return f"{normalized_name}-{revision}"


def playwright_cache_candidates(
    platform_name: str,
    *,
    home: str | Path | None = None,
    local_appdata: str | Path | None = None,
) -> list[Path]:
    normalized_platform = platform_name.lower()
    home_path = Path(home).expanduser() if home is not None else Path.home()
    candidates: list[Path] = []

    if normalized_platform == "darwin":
        candidates.append(home_path / "Library" / "Caches" / "ms-playwright")
    elif normalized_platform.startswith("win"):
        if local_appdata is not None:
            candidates.append(Path(local_appdata) / "ms-playwright")
        candidates.append(home_path / "AppData" / "Local" / "ms-playwright")
    else:
        candidates.append(home_path / ".cache" / "ms-playwright")

    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(resolved)
    return unique_candidates
