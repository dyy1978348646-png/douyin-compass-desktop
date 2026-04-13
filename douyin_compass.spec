# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import platform

from build_support import dedupe_datas, pyinstaller_excludes, pyinstaller_hiddenimports


APP_NAME = "抖音罗盘抓取器"
PROJECT_ROOT = Path(SPECPATH)
ASSETS_DIR = PROJECT_ROOT / "assets"
PLAYWRIGHT_DIR = PROJECT_ROOT / "ms-playwright"
IS_MACOS = platform.system() == "Darwin"

datas = []
if ASSETS_DIR.exists():
    datas.append((str(ASSETS_DIR), "assets"))
if PLAYWRIGHT_DIR.exists():
    datas.append((str(PLAYWRIGHT_DIR), "ms-playwright"))
datas = dedupe_datas(datas)

hiddenimports = pyinstaller_hiddenimports(platform.system().lower())
excludes = pyinstaller_excludes()

icon_path = None
if IS_MACOS and (ASSETS_DIR / "app_icon.icns").exists():
    icon_path = str(ASSETS_DIR / "app_icon.icns")
elif (ASSETS_DIR / "app_icon.ico").exists():
    icon_path = str(ASSETS_DIR / "app_icon.ico")

a = Analysis(
    ["main.py"],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=APP_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=IS_MACOS,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_path,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name=APP_NAME,
)

if IS_MACOS:
    app = BUNDLE(
        coll,
        name=f"{APP_NAME}.app",
        icon=str(ASSETS_DIR / "app_icon.icns") if (ASSETS_DIR / "app_icon.icns").exists() else None,
        bundle_identifier=None,
    )
