"""
运行时环境辅助：
1. 在应用启动前配置 Playwright 浏览器路径
2. 为 Windows 开启 DPI 感知
3. 在 Windows 上隐藏浏览器安装子进程窗口
"""

from __future__ import annotations

import ctypes
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Iterable

IS_WINDOWS = platform.system() == "Windows"
PROJECT_ROOT = Path(__file__).resolve().parent


def resource_base_path() -> Path:
    """返回当前运行环境中的资源根目录。"""
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return PROJECT_ROOT


def executable_dir() -> Path:
    """返回当前可执行文件所在目录。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return PROJECT_ROOT


def has_browser_install(browser_root: Path) -> bool:
    """判断目录下是否存在预装的 Chromium。"""
    return browser_root.exists() and any(
        path.is_dir() for path in browser_root.glob("chromium-*")
    )


def bundled_playwright_browser_root(base_path: Path) -> Path | None:
    """返回 Playwright 包内置浏览器目录。"""
    browser_root = base_path / "playwright" / "driver" / "package" / ".local-browsers"
    if has_browser_install(browser_root):
        return browser_root
    return None


def iter_playwright_browser_candidates() -> list[Path]:
    """返回一组优先级有序的浏览器目录候选路径。"""
    candidates = [
        resource_base_path() / "ms-playwright",
        executable_dir() / "ms-playwright",
        PROJECT_ROOT / "ms-playwright",
    ]

    seen: set[Path] = set()
    ordered: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(candidate)
    return ordered


def pick_playwright_browser_root(
    candidates: Iterable[Path],
    fallback_root: Path,
) -> Path:
    """优先使用已预装的浏览器目录，否则返回可写回退目录。"""
    for candidate in candidates:
        if has_browser_install(candidate):
            return candidate

    fallback_root.mkdir(parents=True, exist_ok=True)
    return fallback_root


def configure_playwright_browser_env(fallback_root: Path) -> Path:
    """
    在 Playwright 初始化前配置浏览器目录。
    预装浏览器优先，缺失时回退到用户可写目录。
    """
    packaged_browser_roots = [
        bundled_playwright_browser_root(resource_base_path()),
        bundled_playwright_browser_root(executable_dir()),
        bundled_playwright_browser_root(executable_dir() / "_internal"),
    ]
    packaged_browser_root = next(
        (browser_root for browser_root in packaged_browser_roots if browser_root),
        None,
    )
    if packaged_browser_root:
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"
        return packaged_browser_root

    browser_root = pick_playwright_browser_root(
        candidates=iter_playwright_browser_candidates(),
        fallback_root=fallback_root,
    )
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_root)
    return browser_root


def build_hidden_subprocess_kwargs() -> dict:
    """Windows 上隐藏子进程窗口，其他平台返回空配置。"""
    if not IS_WINDOWS:
        return {}

    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE

    return {
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000),
        "startupinfo": startupinfo,
    }


def enable_windows_dpi_awareness() -> bool:
    """尽量启用 Windows 高 DPI 感知，避免 Tk 窗口发糊。"""
    if not IS_WINDOWS:
        return False

    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
        return True
    except Exception:
        pass

    try:
        ctypes.windll.user32.SetProcessDPIAware()
        return True
    except Exception:
        return False
