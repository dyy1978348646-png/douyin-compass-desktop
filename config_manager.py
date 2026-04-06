"""
配置管理器 - 加载/保存用户配置（账号、调度时间等）
配置文件保存在程序目录下的 config.json 中，密码使用 base64 编码存储。
注意：base64 不是加密，生产环境建议使用 keyring 库（跨平台，
      macOS 走 Keychain，Windows 走 Credential Manager）。
"""

import base64
import json
import os
import platform
from pathlib import Path

IS_MACOS = platform.system() == "Darwin"
IS_WINDOWS = platform.system() == "Windows"
APP_NAME = "抖音罗盘抓取器"


def _get_runtime_data_dir() -> Path:
    """返回当前环境下可写的应用数据目录。"""
    if not getattr(os.sys, "frozen", False):
        return Path(__file__).parent

    home = Path.home()
    if IS_MACOS:
        return home / "Library" / "Application Support" / APP_NAME
    if IS_WINDOWS:
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / APP_NAME
        return home / "AppData" / "Roaming" / APP_NAME

    return home / f".{APP_NAME}"

# 应用运行数据目录
# - 开发时：项目目录
# - macOS 打包：~/Library/Application Support/抖音罗盘抓取器
# - Windows 打包：%APPDATA%/抖音罗盘抓取器
APP_DIR = _get_runtime_data_dir()
APP_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_FILE = APP_DIR / "config.json"
COOKIE_FILE = APP_DIR / "cookies.json"
DATA_DIR = APP_DIR / "data"
LOG_DIR = APP_DIR / "logs"
DOWNLOAD_DIR = APP_DIR / "downloads"
PLAYWRIGHT_BROWSERS_DIR = APP_DIR / "ms-playwright"

for d in [DATA_DIR, LOG_DIR, DOWNLOAD_DIR, PLAYWRIGHT_BROWSERS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# 默认配置
DEFAULT_CONFIG = {
    "compass_url": "https://compass.jinritemai.com",
    "portal_type": "creator",       # "creator" = 达人入口, "shop" = 店铺入口
    "date_mode": "last_7_days",     # last_7_days | last_1_day | custom_date
    "custom_date_text": "",
    "schedule_enabled": True,
    "schedule_hour": 8,
    "schedule_minute": 0,
    "headless": False,
    "browser_timeout": 60000,
    "download_timeout": 120,
    "login_wait_timeout": 300,
    "sqlite_db": str(DATA_DIR / "douyin_compass.db"),
    "sqlite_table": "rpa_douyin_launch_live",
}


def load_config() -> dict:
    """从 config.json 加载配置，不存在则返回默认值。"""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            # 合并默认值（处理新增字段）
            merged = {**DEFAULT_CONFIG, **saved}
            return merged
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(cfg: dict):
    """保存配置到 config.json。"""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def encode_password(plain: str) -> str:
    """将明文密码编码为 base64 字符串。"""
    return base64.b64encode(plain.encode("utf-8")).decode("utf-8")


def decode_password(b64: str) -> str:
    """将 base64 字符串解码为明文密码。"""
    if not b64:
        return ""
    try:
        return base64.b64decode(b64.encode("utf-8")).decode("utf-8")
    except Exception:
        return ""
