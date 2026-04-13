"""
打包辅助配置，供 PyInstaller spec 和测试共用。
"""

from __future__ import annotations

from pathlib import Path

APP_NAME = "抖音罗盘抓取器"

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


def windows_release_dir_name(app_name: str = APP_NAME) -> str:
    return f"{app_name}_Windows版"


def render_windows_launcher_bat(app_exe_name: str) -> str:
    return (
        "@echo off\r\n"
        "setlocal\r\n"
        "cd /d \"%~dp0\"\r\n"
        f"start \"\" \"%~dp0{app_exe_name}\"\r\n"
    )


def render_windows_quickstart_text(
    *,
    app_name: str = APP_NAME,
    launcher_name: str,
) -> str:
    return (
        f"{app_name} - Windows 使用说明\n"
        "====================================\n\n"
        "1. 先完整解压整个压缩包，不要直接在压缩包里运行。\n"
        f"2. 进入解压后的文件夹，双击“{launcher_name}”。\n"
        "3. 首次启动如果稍慢，属于正常现象，请等待窗口出现。\n"
        "4. 无需安装 Python，也无需命令行操作。\n\n"
        "如果系统提示安全告警，请选择“更多信息”后继续运行。\n"
    )
