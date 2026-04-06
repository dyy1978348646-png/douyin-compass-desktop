"""
抖音罗盘数据抓取器 - 入口文件
双击 main.py 或打包后的 .exe / .app 即可启动 GUI。
支持平台: Windows / macOS (Apple Silicon & Intel)
"""

import logging
import sys

from config_manager import LOG_DIR, IS_MACOS, PLAYWRIGHT_BROWSERS_DIR
from runtime_env import configure_playwright_browser_env, enable_windows_dpi_awareness

# ============================================================
# 全局日志初始化
# ============================================================
from datetime import datetime

logger = logging.getLogger("douyin_rpa")
logger.setLevel(logging.DEBUG)

# 控制台输出
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(console_handler)

# 文件输出
log_file = LOG_DIR / f"rpa_{datetime.now():%Y%m%d}.log"
file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(file_handler)


# ============================================================
# 启动 GUI
# ============================================================
def main():
    configure_playwright_browser_env(PLAYWRIGHT_BROWSERS_DIR)
    enable_windows_dpi_awareness()

    # macOS: 确保在高 DPI 下正确渲染
    if IS_MACOS:
        try:
            from Foundation import NSBundle
            bundle = NSBundle.mainBundle()
            info = bundle.localizedInfoDictionary() or bundle.infoDictionary()
            if info:
                info["CFBundleName"] = "抖音罗盘抓取器"
        except ImportError:
            pass  # pyobjc 未安装时忽略

    from gui_app import MainWindow

    app = MainWindow()
    app.run()


if __name__ == "__main__":
    main()
