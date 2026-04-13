"""
发行包辅助脚本：
1. 整理 Windows 便携版目录
2. 生成双击启动脚本与新手说明
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from build_support import (
    APP_NAME,
    render_windows_launcher_bat,
    render_windows_quickstart_text,
    windows_release_dir_name,
)


def prepare_windows_release(dist_dir: Path, app_name: str = APP_NAME) -> Path:
    source_dir = dist_dir / app_name
    if not source_dir.exists():
        raise FileNotFoundError(f"未找到 PyInstaller 输出目录: {source_dir}")

    release_dir = dist_dir / windows_release_dir_name(app_name)
    if release_dir.exists():
        shutil.rmtree(release_dir, ignore_errors=True)

    shutil.copytree(source_dir, release_dir)

    launcher_name = f"双击启动-{app_name}.bat"
    (release_dir / launcher_name).write_text(
        render_windows_launcher_bat(f"{app_name}.exe"),
        encoding="utf-8",
        newline="",
    )

    (release_dir / "Windows使用说明.txt").write_text(
        render_windows_quickstart_text(
            app_name=app_name,
            launcher_name=launcher_name,
        ),
        encoding="utf-8",
    )

    return release_dir


def main(argv: list[str]) -> int:
    command = argv[1] if len(argv) > 1 else ""
    project_root = Path(__file__).resolve().parent
    dist_dir = project_root / "dist"

    if command == "windows":
        release_dir = prepare_windows_release(dist_dir)
        print(f"Windows 便携版目录已生成: {release_dir}")
        return 0

    print("用法: python build_release_assets.py windows")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
