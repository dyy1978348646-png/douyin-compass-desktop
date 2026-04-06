"""
将本机 Playwright 浏览器缓存复制到当前 Python 环境的包内目录。

优先复用已有缓存，避免每次在干净虚拟环境里重新下载 Chromium。
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from pathlib import Path

import playwright

from build_support import (
    playwright_browser_directory_name,
    playwright_cache_candidates,
)

SUPPORTED_BROWSER_NAMES = {
    "chromium",
    "chromium-headless-shell",
    "ffmpeg",
}


def package_browser_root() -> Path:
    package_root = Path(playwright.__file__).resolve().parent
    return package_root / "driver" / "package" / ".local-browsers"


def browsers_json_path() -> Path:
    package_root = Path(playwright.__file__).resolve().parent
    return package_root / "driver" / "package" / "browsers.json"


def read_browser_specs() -> list[dict[str, str]]:
    data = json.loads(browsers_json_path().read_text(encoding="utf-8"))
    return [
        {"name": browser["name"], "revision": str(browser["revision"])}
        for browser in data["browsers"]
        if browser["name"] in SUPPORTED_BROWSER_NAMES
    ]


def cache_roots() -> list[Path]:
    override = os.environ.get("PLAYWRIGHT_BROWSER_CACHE")
    if override:
        return [Path(override).expanduser()]

    return playwright_cache_candidates(
        platform.system(),
        home=Path.home(),
        local_appdata=os.environ.get("LOCALAPPDATA"),
    )


def copy_browser_tree(source: Path, target: Path) -> None:
    if target.exists():
        return
    shutil.copytree(source, target, symlinks=True)


def main() -> int:
    target_root = package_browser_root()
    target_root.mkdir(parents=True, exist_ok=True)

    missing_specs = read_browser_specs()
    available_cache_roots = [path for path in cache_roots() if path.exists()]
    if not available_cache_roots:
        print("[INFO] 未找到本机 Playwright 浏览器缓存，回退到在线安装。")
        return 1

    for spec in missing_specs:
        directory_name = playwright_browser_directory_name(
            spec["name"],
            spec["revision"],
        )
        target_dir = target_root / directory_name
        if target_dir.exists():
            continue

        for cache_root in available_cache_roots:
            source_dir = cache_root / directory_name
            if not source_dir.exists():
                continue
            print(f"[INFO] 复制缓存浏览器: {source_dir} -> {target_dir}")
            copy_browser_tree(source_dir, target_dir)
            break

    missing_directories = [
        playwright_browser_directory_name(spec["name"], spec["revision"])
        for spec in missing_specs
        if not (target_root / playwright_browser_directory_name(spec["name"], spec["revision"])).exists()
    ]
    if missing_directories:
        print("[INFO] 本机缓存不完整，仍需在线安装:")
        for directory_name in missing_directories:
            print(f"  - {directory_name}")
        return 1

    print(f"[INFO] 已复用本机缓存到 {target_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
