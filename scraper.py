"""
抖音罗盘数据抓取器 - 浏览器自动化核心逻辑

完整流程：
  1. 加载 Cookie → 打开罗盘首页
  2. 等待用户完成所有前置操作（入口选择、登录、账号选择）
     程序不强行检测每一步，只等最终结果：进入仪表盘
  3. 进入仪表盘后保存 Cookie
  4. 根据入口类型（达人/店铺）导航到对应的数据模块
  5. 导出数据并保存

入口类型区别：
  - 达人入口：直播 → 直播复盘
  - 店铺入口：直播 → 实时直播数据
"""

import json
import logging
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright._impl._driver import compute_driver_executable, get_driver_env
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from config_manager import COOKIE_FILE, DOWNLOAD_DIR, APP_DIR, PLAYWRIGHT_BROWSERS_DIR
from scraper_logic import (
    ACCOUNT_SELECTION_KEYWORDS,
    DASHBOARD_NAV_TEXTS,
    PORTAL_SELECTION_KEYWORDS,
    detect_page_status,
    is_account_selection_page_snapshot,
    is_dashboard_page_snapshot,
    is_target_date_visible,
)
from scraper_storage import persist_exported_dataframe
from runtime_env import (
    build_hidden_subprocess_kwargs,
    configure_playwright_browser_env,
    has_browser_install,
)
from scraper_waits import wait_for_page_ready, wait_until

logger = logging.getLogger("douyin_rpa")


def ensure_playwright_browser_installed():
    """
    优先使用打包阶段预装的 Chromium，缺失时再回退到用户目录安装。
    """
    browser_root = configure_playwright_browser_env(PLAYWRIGHT_BROWSERS_DIR)

    if has_browser_install(browser_root):
        installed = next(path for path in browser_root.glob("chromium-*") if path.is_dir())
        logger.info(f"检测到本地 Chromium: {installed}")
        return

    if getattr(sys, "frozen", False):
        logger.warning("未检测到随应用打包的 Chromium，回退到用户目录执行安装")
    else:
        logger.info("开发环境未检测到 Chromium，准备执行本地安装...")

    browser_root.mkdir(parents=True, exist_ok=True)

    driver_executable, driver_cli = compute_driver_executable()
    env = get_driver_env()
    env["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_root)

    completed = subprocess.run(
        [driver_executable, driver_cli, "install", "chromium"],
        env=env,
        capture_output=True,
        text=True,
        **build_hidden_subprocess_kwargs(),
    )

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or "未知错误"
        raise RuntimeError(
            "Chromium 浏览器组件安装失败。"
            "请确认当前电脑可以联网，或稍后重试。\n"
            f"安装输出: {detail}"
        )

    logger.info("Chromium 浏览器组件安装完成")


def parse_user_date_text(user_text: str, today: date | None = None) -> date:
    """支持常见中文和数字日期表达。"""
    raw = (user_text or "").strip()
    if not raw:
        raise RuntimeError("自定义日期不能为空，请输入如 2026-03-28 或 3月28日")

    today = today or datetime.now().date()
    normalized = raw.replace("：", ":").replace(".", "-").replace("/", "-")

    special_dates = {
        "今天": today,
        "今日": today,
        "昨天": today - timedelta(days=1),
        "昨日": today - timedelta(days=1),
        "前天": today - timedelta(days=2),
    }
    if normalized in special_dates:
        return special_dates[normalized]

    full_match = re.fullmatch(r"(\d{4})[-年](\d{1,2})[-月](\d{1,2})日?", normalized)
    if full_match:
        year, month, day = map(int, full_match.groups())
        return date(year, month, day)

    short_match = re.fullmatch(r"(\d{1,2})[-月](\d{1,2})日?", normalized)
    if short_match:
        month, day = map(int, short_match.groups())
        return date(today.year, month, day)

    compact_match = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", normalized)
    if compact_match:
        year, month, day = map(int, compact_match.groups())
        return date(year, month, day)

    raise RuntimeError(
        f"无法识别日期“{raw}”。请使用如 2026-03-28、2026年3月28日、3月28日、昨天。"
    )


# ============================================================
# Cookie 管理
# ============================================================
def save_cookies(context, path: Path = COOKIE_FILE):
    """将浏览器上下文的 Cookie 保存到文件。"""
    cookies = context.cookies()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)
    logger.info(f"Cookie 已保存 ({len(cookies)} 条)")


def load_cookies(context, path: Path = COOKIE_FILE) -> bool:
    """从文件加载 Cookie 到浏览器上下文。返回是否成功。"""
    if not path.exists():
        logger.info("未找到已保存的 Cookie")
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        if not cookies:
            return False
        context.add_cookies(cookies)
        logger.info(f"已加载 Cookie ({len(cookies)} 条)")
        return True
    except Exception as e:
        logger.warning(f"Cookie 加载失败: {e}")
        return False


# ============================================================
# 核心抓取类
# ============================================================
class DouyinCompassScraper:
    """封装抖音罗盘的浏览器自动化操作。"""

    # 入口类型常量
    PORTAL_CREATOR = "creator"   # 达人入口
    PORTAL_SHOP = "shop"         # 店铺入口

    def __init__(self, config: dict):
        self.config = config
        self.pw = None
        self.browser = None
        self.context = None
        self.page = None
        self._cancelled = False
        self.account_name = ""  # 当前登录的账号名称

    def cancel(self):
        """外部调用以取消正在执行的任务。"""
        self._cancelled = True

    def _check_cancel(self):
        if self._cancelled:
            raise InterruptedError("任务已被用户取消")

    @property
    def portal_type(self) -> str:
        """获取入口类型配置，默认达人入口。"""
        return self.config.get("portal_type", self.PORTAL_CREATOR)

    def run_switch_account(self) -> dict:
        """
        切换账号流程（手动确认模式）：
          1. 打开浏览器，导航到罗盘页面
          2. 等待用户在浏览器中完成账号切换
          3. 用户在 GUI 中点击「确认已切换」后关闭浏览器
        不做任何自动检测，完全由用户控制节奏。
        """
        self._cancelled = False
        self._switch_confirmed = False
        result = {"success": False, "message": ""}

        with sync_playwright() as pw:
            self.pw = pw
            try:
                ensure_playwright_browser_installed()
                self._start_browser()
                self._check_cancel()

                # 关闭旧标签页，保持干净
                self._close_extra_tabs()

                url = self.config.get("compass_url", "https://compass.jinritemai.com")
                logger.info(f"打开罗盘: {url}")
                self.page.goto(url, wait_until="networkidle")
                wait_for_page_ready(self.page)

                logger.info("=" * 50)
                logger.info("请在浏览器中完成账号切换：")
                logger.info("  1. 等待罗盘页面加载完成")
                logger.info("  2. 点击右上角头像 → 切换账号")
                logger.info("  3. 选择要使用的新账号")
                logger.info("  4. 等待新账号的罗盘页面加载完成")
                logger.info("  5. 回到本程序，点击「确认已切换」按钮")
                logger.info("=" * 50)

                # 等待用户在 GUI 中点击「确认已切换」
                timeout = self.config.get("login_wait_timeout", 300)
                def switch_confirmed() -> bool:
                    self._check_cancel()
                    return self._switch_confirmed

                try:
                    wait_until(
                        switch_confirmed,
                        timeout_seconds=timeout,
                        interval_seconds=1,
                        timeout_message=f"等待超时（{timeout}秒）",
                    )
                    logger.info("用户确认账号切换完成!")
                    result["success"] = True
                    result["message"] = "账号切换成功"
                except TimeoutError:
                    result["message"] = f"等待超时（{timeout}秒）"
                    logger.warning(result["message"])

            except InterruptedError:
                result["message"] = "已取消"
                logger.info("账号切换已取消")
            except Exception as e:
                result["message"] = f"失败: {e}"
                logger.exception(f"账号切换失败: {e}")
            finally:
                self._close()

        return result

    def confirm_switch(self):
        """GUI 调用：用户确认账号已切换完成。"""
        self._switch_confirmed = True

    def _close_extra_tabs(self):
        """
        关闭多余的标签页，只保留一个。
        持久化浏览器启动时会恢复上次所有标签页，
        旧标签页上的仪表盘会干扰切换账号的检测逻辑。
        """
        pages = self.context.pages
        if len(pages) <= 1:
            return

        logger.info(f"检测到 {len(pages)} 个标签页，关闭多余标签页...")
        # 保留第一个页面，关闭其余的
        for page in pages[1:]:
            try:
                page.close()
            except Exception:
                pass

        # 确保 self.page 指向唯一剩余的页面
        if self.context.pages:
            self.page = self.context.pages[0]
        else:
            self.page = self.context.new_page()

        logger.info("已清理旧标签页")

    def _try_trigger_account_switch(self):
        """尝试在页面上找到并点击账号切换入口。"""
        # 常见的账号切换入口：右上角头像区域、账号名称等
        switch_selectors = [
            # 直接切换账号链接
            'a:has-text("切换账号")',
            'span:has-text("切换账号")',
            'div:has-text("切换账号")',
            # 右上角头像/用户菜单（点击后可能出现切换选项）
            '[class*="avatar"]',
            '[class*="user-info"]',
            '[class*="account"]',
            '[class*="profile"]',
        ]
        # 先尝试直接点击"切换账号"
        for sel in switch_selectors[:3]:
            try:
                el = self.page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    wait_for_page_ready(self.page, timeout_ms=5000)
                    logger.info("已点击'切换账号'入口")
                    return
            except Exception:
                continue

        # 尝试点击头像区域（可能触发下拉菜单）
        for sel in switch_selectors[3:]:
            try:
                el = self.page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    wait_for_page_ready(self.page, timeout_ms=5000)
                    # 点击头像后查找切换账号选项
                    for text_sel in ['text="切换账号"', 'text="切换主体"', 'text="切换店铺"']:
                        try:
                            switch_el = self.page.query_selector(text_sel)
                            if switch_el and switch_el.is_visible():
                                switch_el.click()
                                wait_for_page_ready(self.page, timeout_ms=5000)
                                logger.info("已通过头像菜单触发账号切换")
                                return
                        except Exception:
                            continue
            except Exception:
                continue

        logger.info("未能自动触发账号切换，请在浏览器中手动操作")

    def run(self) -> dict:
        """执行完整抓取流程，返回结果摘要。"""
        self._cancelled = False
        result = {
            "success": False,
            "rows": 0,
            "message": "",
            "filepath": "",
            "csv_path": "",
        }

        portal_name = "达人" if self.portal_type == self.PORTAL_CREATOR else "店铺"
        logger.info(f"当前入口类型: {portal_name}")

        with sync_playwright() as pw:
            self.pw = pw
            try:
                ensure_playwright_browser_installed()
                self._start_browser()
                self._check_cancel()
                self._open_and_wait_for_dashboard()
                self._check_cancel()
                self.account_name = self._get_account_name()
                self._navigate_to_data_module()
                self._check_cancel()
                self._apply_date_selection()
                self._check_cancel()
                filepath = self._export_data()
                self._check_cancel()
                df, csv_path = self._process_and_save(filepath)

                result["success"] = True
                result["rows"] = len(df)
                result["filepath"] = str(filepath)
                result["csv_path"] = str(csv_path)
                result["account_name"] = self.account_name
                result["message"] = f"成功抓取 {len(df)} 行数据"
                if self.account_name:
                    result["message"] += f"（账号: {self.account_name}）"
                logger.info(result["message"])

            except InterruptedError:
                result["message"] = "任务已取消"
                logger.info(result["message"])
            except Exception as e:
                result["message"] = f"失败: {e}"
                logger.exception(f"任务执行失败: {e}")
            finally:
                self._close()

        return result

    # ------ 内部方法 ------

    def _start_browser(self):
        """
        启动浏览器，使用持久化用户配置文件。

        持久化模式的好处：
          - 浏览器会保留 Cookie、localStorage、sessionStorage、登录状态
          - 登录一次后，下次打开自动保持登录，和你日常用的浏览器一样
          - 不需要手动导入/导出 Cookie 文件
        """
        headless = self.config.get("headless", False)

        # 浏览器配置文件目录（持久化存储在程序目录下）
        user_data_dir = APP_DIR / "browser_profile"
        user_data_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"启动浏览器 (headless={headless}, 持久化配置: {user_data_dir})")

        self.context = self.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080},
            locale="zh-CN",
        )
        self.context.set_default_timeout(self.config.get("browser_timeout", 60000))

        # persistent context 自带一个页面，如果没有就新建
        if self.context.pages:
            self.page = self.context.pages[0]
        else:
            self.page = self.context.new_page()

    def _open_and_wait_for_dashboard(self):
        """
        打开罗盘网站，等待用户完成所有前置操作后进入仪表盘。

        不再逐步检测"当前在哪个页面"，而是：
          1. 打开网站
          2. 每隔几秒检查一次是否已到达仪表盘
          3. 期间持续在日志中提示当前状态
          4. 到达仪表盘后继续执行

        这样无论中间经过多少步（入口选择、登录、账号选择、验证码等），
        程序都不会误判，只要最终进入仪表盘就行。
        """
        url = self.config.get("compass_url", "https://compass.jinritemai.com")

        # 持久化模式下不需要手动加载 Cookie，浏览器配置文件自动保留登录状态

        logger.info(f"打开罗盘: {url}")
        self.page.goto(url, wait_until="networkidle")
        wait_for_page_ready(self.page)

        # 快速检查：持久化配置保留了登录状态，可能直接进入仪表盘
        if self._is_on_dashboard():
            logger.info("已登录，直接进入仪表盘")
            return

        # 需要用户手动操作
        logger.info("=" * 50)
        logger.info("请在浏览器中完成以下操作：")
        logger.info("  1. 选择入口（达人入口 / 店铺入口）")
        logger.info("  2. 手机验证码登录（如需要）")
        logger.info("  3. 选择账号（如有多个）")
        logger.info("完成后程序会自动继续，请勿关闭浏览器窗口")
        logger.info("下次运行时浏览器会记住登录状态，无需重复登录")
        logger.info("=" * 50)

        timeout = self.config.get("login_wait_timeout", 300)
        last_status = {"value": ""}

        def dashboard_ready() -> bool:
            self._check_cancel()

            # 检测当前状态并记录日志（避免重复打印）
            status = self._detect_page_status()
            if status != last_status["value"]:
                logger.info(f"当前页面状态: {status}")
                last_status["value"] = status

            return self._is_on_dashboard()

        wait_until(
            dashboard_ready,
            timeout_seconds=timeout,
            interval_seconds=1.5,
            timeout_message=(
                f"等待进入仪表盘超时（{timeout}秒）。"
                f"请确保在浏览器中完成登录和账号选择。"
            ),
        )
        logger.info("已进入仪表盘!")

    def _visible_keywords_for_page(self, page, keywords: tuple[str, ...]) -> list[str]:
        visible_keywords = []
        for keyword in keywords:
            try:
                element = page.query_selector(f'text="{keyword}"')
                if element and element.is_visible():
                    visible_keywords.append(keyword)
            except Exception:
                continue
        return visible_keywords

    def _collect_dashboard_visible_texts(self, page) -> list[str]:
        keywords = tuple(
            dict.fromkeys(PORTAL_SELECTION_KEYWORDS + ACCOUNT_SELECTION_KEYWORDS + DASHBOARD_NAV_TEXTS)
        )
        return self._visible_keywords_for_page(page, keywords)

    def _detect_page_status(self) -> str:
        """检测当前页面大致状态，用于日志输出。"""
        visible_texts = self._collect_dashboard_visible_texts(self.page)
        return detect_page_status(self.page.url, visible_texts)

    def _is_on_dashboard(self) -> bool:
        """
        判断当前页面是否是罗盘仪表盘。
        会检查所有打开的标签页（登录过程中可能在新标签页打开仪表盘）。

        实际罗盘主页特征：
          - URL: compass.jinritemai.com/talent (达人端)
          - 顶部导航栏有: 首页、直播、短视频、图文、橱窗、交易、商品、商家、人群、市场
        """
        # 检查所有标签页，不只是当前页
        for page in self.context.pages:
            if self._check_page_is_dashboard(page):
                # 如果仪表盘在另一个标签页，切换过去
                if page != self.page:
                    logger.info("检测到仪表盘在新标签页，已切换")
                    self.page = page
                return True
        return False

    def _check_page_is_dashboard(self, page) -> bool:
        """检查指定页面是否是罗盘仪表盘。"""
        try:
            current_url = page.url
        except Exception:
            return False

        visible_texts = self._collect_dashboard_visible_texts(page)
        return is_dashboard_page_snapshot(current_url, visible_texts)

    def _is_account_selection_page(self, page) -> bool:
        """
        检测页面是否是账号选择页。
        切换账号后会进入账号列表页面，用户需要选择具体账号。
        这个页面不应被判定为仪表盘。
        """
        try:
            visible_texts = self._visible_keywords_for_page(page, ACCOUNT_SELECTION_KEYWORDS)
            return is_account_selection_page_snapshot(page.url, visible_texts)
        except Exception:
            return False

    def _get_account_name(self) -> str:
        """
        从仪表盘页面提取当前登录的账号名称。
        通常在页面右上角头像旁边、或页面顶栏显示账号/店铺/达人名称。
        """
        name_selectors = [
            # 右上角用户名/店铺名/达人名
            '[class*="user-name"]',
            '[class*="username"]',
            '[class*="shop-name"]',
            '[class*="shopName"]',
            '[class*="account-name"]',
            '[class*="accountName"]',
            '[class*="nick-name"]',
            '[class*="nickName"]',
            '[class*="talent-name"]',
            '[class*="talentName"]',
            # 头像旁的文字
            '[class*="avatar"] + span',
            '[class*="avatar"] + div',
            '[class*="header-user"] span',
            '[class*="header-right"] span',
            '[class*="user-info"] span',
            '[class*="userInfo"] span',
        ]
        for sel in name_selectors:
            try:
                el = self.page.query_selector(sel)
                if el and el.is_visible():
                    text = el.inner_text().strip()
                    if text and len(text) <= 30:
                        logger.info(f"检测到当前账号: {text}")
                        return text
            except Exception:
                continue

        # 备选：尝试从页面 title 中提取
        try:
            title = self.page.title()
            if title and "罗盘" in title:
                # 标题可能类似 "xxx的罗盘" 或 "罗盘 - xxx"
                for sep in [" - ", "的罗盘", "-"]:
                    if sep in title:
                        parts = title.split(sep)
                        candidate = parts[0].strip() if sep != "的罗盘" else parts[0].strip()
                        if candidate and candidate != "罗盘" and len(candidate) <= 20:
                            logger.info(f"从页面标题检测到账号: {candidate}")
                            return candidate
        except Exception:
            pass

        logger.warning("未能自动识别当前账号名称")
        return ""

    # ============================================================
    # 导航 - 根据入口类型走不同路径
    # ============================================================
    def _navigate_to_data_module(self):
        """根据入口类型导航到对应的数据模块。"""
        if self.portal_type == self.PORTAL_CREATOR:
            self._navigate_creator_live_review()
        else:
            self._navigate_shop_live_data()

    def _navigate_creator_live_review(self):
        """
        达人入口导航：顶部导航栏点击"直播" → 子菜单点击"直播复盘"
        罗盘顶部导航栏有: 首页、直播、短视频、图文、橱窗、交易、商品、商家、人群、市场
        """
        logger.info("【达人端】导航到 直播 → 直播复盘...")

        # 第一步：点击顶部导航的"直播"标签
        live_selectors = [
            # 顶部导航栏的标签链接（优先精确匹配）
            'nav a:has-text("直播")',
            'header a:has-text("直播")',
            '[class*="tab"]:has-text("直播")',
            '[class*="nav"] a:has-text("直播")',
            '[class*="header"] a:has-text("直播")',
            '[class*="menu"] a:has-text("直播")',
            'a:has-text("直播")',
            'span:has-text("直播")',
        ]
        try:
            live_nav = self._find_element(live_selectors, "直播菜单")
            live_nav.click()
            wait_for_page_ready(self.page, timeout_ms=8000)
            logger.info("已点击'直播'标签")
        except RuntimeError:
            logger.info("未找到'直播'标签，尝试直接查找'直播复盘'...")

        # 第二步：点击"直播复盘"（可能在子菜单、侧边栏或页面内）
        review_selectors = [
            'a:has-text("直播复盘")',
            'span:has-text("直播复盘")',
            '[class*="menu"]:has-text("直播复盘")',
            '[class*="nav"]:has-text("直播复盘")',
            '[class*="tab"]:has-text("直播复盘")',
            'li:has-text("直播复盘")',
            'div:has-text("直播复盘")',
        ]
        try:
            review_link = self._find_element(review_selectors, "直播复盘")
            review_link.click()
            wait_for_page_ready(self.page, timeout_ms=8000)
            logger.info("已进入直播复盘页面")
            return
        except RuntimeError:
            pass

        # 备选：直接通过 URL 访问
        url = self.config.get("compass_url", "https://compass.jinritemai.com")
        fallback_urls = [
            f"{url}/creator/live/review",
            f"{url}/live/review",
            f"{url}/creator/live/replay",
        ]
        for fb_url in fallback_urls:
            logger.info(f"尝试直接访问: {fb_url}")
            try:
                self.page.goto(fb_url, wait_until="networkidle", timeout=15000)
                wait_for_page_ready(self.page, timeout_ms=8000)
                # 检查页面是否加载成功（不是 404 或空白）
                if "404" not in self.page.title().lower():
                    logger.info(f"已通过 URL 进入直播复盘页面")
                    return
            except Exception:
                continue

        raise RuntimeError(
            "无法进入直播复盘页面。请确认达人罗盘中是否有「直播 → 直播复盘」菜单。"
        )

    def _navigate_shop_live_data(self):
        """
        店铺入口导航：顶部导航栏点击"直播" → 子菜单点击"实时直播数据"
        """
        logger.info("【店铺端】导航到 直播 → 实时直播数据...")

        # 第一步：点击顶部导航的"直播"标签
        live_selectors = [
            'nav a:has-text("直播")',
            'header a:has-text("直播")',
            '[class*="tab"]:has-text("直播")',
            '[class*="nav"] a:has-text("直播")',
            '[class*="header"] a:has-text("直播")',
            'a:has-text("直播")',
            'span:has-text("直播")',
        ]
        try:
            live_nav = self._find_element(live_selectors, "直播菜单")
            live_nav.click()
            wait_for_page_ready(self.page, timeout_ms=8000)
        except RuntimeError:
            logger.info("未找到'直播'标签，继续查找子菜单...")

        # 第二步：点击"实时直播数据"
        realtime_selectors = [
            'a:has-text("实时直播数据")',
            'span:has-text("实时直播数据")',
            '[class*="menu"]:has-text("实时直播数据")',
            'li:has-text("实时直播数据")',
        ]
        try:
            link = self._find_element(realtime_selectors, "实时直播数据")
            link.click()
            wait_for_page_ready(self.page, timeout_ms=8000)
            logger.info("已进入实时直播数据页面")
            return
        except RuntimeError:
            pass

        # 备选：直接通过 URL 访问
        url = self.config.get("compass_url", "https://compass.jinritemai.com")
        fallback_urls = [
            f"{url}/live/realtime",
            f"{url}/shop/live/realtime",
        ]
        for fb_url in fallback_urls:
            logger.info(f"尝试直接访问: {fb_url}")
            try:
                self.page.goto(fb_url, wait_until="networkidle", timeout=15000)
                wait_for_page_ready(self.page, timeout_ms=8000)
                if "404" not in self.page.title().lower():
                    logger.info(f"已通过 URL 进入实时直播数据页面")
                    return
            except Exception:
                continue

        raise RuntimeError(
            "无法进入实时直播数据页面。请确认店铺罗盘中是否有「直播 → 实时直播数据」菜单。"
        )

    # ============================================================
    # 数据导出和保存
    # ============================================================
    def _apply_date_selection(self):
        """根据配置选择日期范围。"""
        mode = self.config.get("date_mode", "last_7_days")

        if mode == "last_1_day":
            self._select_quick_date_range(
                "近一天",
                ["近1天", "近一天", "昨天", "昨日"],
            )
            return

        if mode == "custom_date":
            self._select_custom_date(self.config.get("custom_date_text", ""))
            return

        self._select_quick_date_range(
            "近七天",
            ["近7天", "最近7天", "近七天", "最近七天", "近7日", "最近7日"],
        )

    def _select_quick_date_range(self, display_name: str, texts: list[str]):
        logger.info(f"选择日期范围: {display_name}")
        selectors = []
        for text in texts:
            selectors.extend([
                f'span:has-text("{text}")',
                f'button:has-text("{text}")',
                f'div:has-text("{text}")',
                f'li:has-text("{text}")',
                f'a:has-text("{text}")',
            ])

        btn = self._find_element(selectors, f"{display_name}按钮")
        btn.click()
        wait_for_page_ready(self.page, timeout_ms=5000)

    def _select_custom_date(self, user_text: str):
        """解析自然语言日期，并尽量填入页面日期控件。"""
        target_date = parse_user_date_text(user_text)
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"选择自定义日期: {user_text} -> {date_str}")

        trigger_selectors = [
            'input[placeholder*="日期"]',
            'input[placeholder*="开始"]',
            'input[placeholder*="选择"]',
            '[class*="date-picker"]',
            '[class*="picker"] input',
            '[class*="range"] input',
        ]

        for selector in trigger_selectors:
            try:
                el = self.page.query_selector(selector)
                if el and el.is_visible():
                    el.click()
                    wait_for_page_ready(self.page, timeout_ms=3000)
                    break
            except Exception:
                continue

        range_inputs = self.page.locator(
            'input[placeholder*="开始"], '
            'input[placeholder*="结束"], '
            'input[placeholder*="日期"], '
            'input[placeholder*="选择日期"], '
            'input[placeholder*="Start"], '
            'input[placeholder*="End"]'
        )

        try:
            count = range_inputs.count()
        except Exception:
            count = 0

        if count >= 2:
            first = range_inputs.nth(0)
            second = range_inputs.nth(1)
            self._fill_date_input(first, date_str)
            self._fill_date_input(second, date_str)
            second.press("Enter")
            wait_for_page_ready(self.page, timeout_ms=5000)
            self._assert_date_selection_applied(target_date)
            return

        if count == 1:
            only = range_inputs.nth(0)
            if self._try_fill_single_date_input(only, date_str, target_date):
                return

        raise RuntimeError(f"无法自动设置页面日期，请手动选择 {date_str} 后再执行")

    def _fill_date_input(self, locator, value: str):
        locator.click()
        locator.fill("")
        locator.fill(value)

    def _try_fill_single_date_input(self, locator, date_str: str, target_date: date) -> bool:
        attempts = [date_str, f"{date_str} - {date_str}"]
        for attempt in attempts:
            try:
                locator.click()
                locator.fill("")
                locator.fill(attempt)
                locator.press("Enter")
                wait_for_page_ready(self.page, timeout_ms=5000)
                self._assert_date_selection_applied(target_date)
                return True
            except Exception:
                continue

        return False

    def _assert_date_selection_applied(self, target_date: date):
        haystacks = []
        try:
            inputs = self.page.locator("input")
            count = inputs.count()
            for idx in range(min(count, 8)):
                loc = inputs.nth(idx)
                try:
                    if loc.is_visible(timeout=500):
                        value = loc.input_value().strip()
                        if value:
                            haystacks.append(value)
                    else:
                        continue
                except Exception:
                    continue
        except Exception:
            pass

        try:
            date_text_blocks = self.page.locator(
                '[class*="date"], [class*="picker"], [class*="range"], [class*="calendar"]'
            )
            count = date_text_blocks.count()
            for idx in range(min(count, 8)):
                loc = date_text_blocks.nth(idx)
                try:
                    if loc.is_visible(timeout=500):
                        text = loc.inner_text().strip()
                        if text:
                            haystacks.append(text)
                except Exception:
                    continue
        except Exception:
            pass

        if is_target_date_visible(target_date, haystacks):
            logger.info(f"页面日期已确认切换到目标日期: {target_date:%Y-%m-%d}")
            return

        raise RuntimeError(
            f"页面上未检测到目标日期 {target_date:%Y-%m-%d}，为避免导出错误数据，本次任务已终止"
        )

    def _export_data(self) -> Path:
        logger.info("导出数据...")
        selectors = [
            'button:has-text("导出")', 'span:has-text("导出")',
            'a:has-text("导出")', '[class*="export"]',
            'button:has-text("下载")',
        ]
        export_btn = self._find_element(selectors, "导出按钮")

        timeout_ms = self.config.get("download_timeout", 120) * 1000
        with self.page.expect_download(timeout=timeout_ms) as dl_info:
            export_btn.click()

        download = dl_info.value
        original_name = download.suggested_filename or "直播数据.xlsx"
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = Path(original_name).stem
        suffix = Path(original_name).suffix or ".xlsx"
        save_path = DOWNLOAD_DIR / f"{stem}_{date_str}{suffix}"

        download.save_as(str(save_path))
        logger.info(f"文件已下载: {save_path}")
        return save_path

    def _process_and_save(self, filepath: Path) -> tuple[pd.DataFrame, Path]:
        logger.info(f"解析: {filepath}")
        df = pd.read_excel(filepath, engine="openpyxl")
        logger.info(f"读取 {len(df)} 行, {len(df.columns)} 列")
        portal_tag = "creator" if self.portal_type == self.PORTAL_CREATOR else "shop"
        return persist_exported_dataframe(
            df,
            portal_type=portal_tag,
            account_name=self.account_name,
            config=self.config,
        )

    def _close(self):
        """关闭浏览器上下文。持久化模式下会自动保存所有浏览器数据。"""
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
            logger.info("浏览器已关闭（登录状态已保存）")

    def _find_element(self, selectors: list, name: str):
        for sel in selectors:
            try:
                el = self.page.query_selector(sel)
                if el and el.is_visible():
                    return el
            except Exception:
                continue
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if loc.is_visible(timeout=3000):
                    return loc
            except Exception:
                continue
        raise RuntimeError(f"无法找到: {name}")
