"""
纯逻辑判断，避免将页面识别规则硬编码在抓取主流程里。
"""

from __future__ import annotations

from datetime import date

LOGIN_URL_KEYWORDS = ("login", "passport", "sso", "sign", "verify", "auth")
PORTAL_SELECTION_KEYWORDS = ("达人入口", "商家入口", "店铺入口")
ACCOUNT_SELECTION_KEYWORDS = (
    "选择账号",
    "切换账号",
    "选择主体",
    "选择店铺",
    "请选择",
    "选择身份",
    "账号列表",
    "主体列表",
)
ACCOUNT_SELECTION_PATH_KEYWORDS = ("select", "choose", "switch", "/account", "/identity", "/role")
DASHBOARD_PATH_KEYWORDS = (
    "/talent",
    "/creator",
    "/dashboard",
    "/overview",
    "/live",
    "/shop/",
    "/home",
    "/content",
    "/traffic",
    "/data",
)
DASHBOARD_NAV_TEXTS = ("直播", "短视频", "橱窗", "商品", "交易")


def _normalize_visible_texts(visible_texts: list[str]) -> list[str]:
    return [text.strip() for text in visible_texts if text and text.strip()]


def _has_visible_keyword(visible_texts: list[str], keywords: tuple[str, ...]) -> bool:
    normalized = _normalize_visible_texts(visible_texts)
    return any(keyword in text for keyword in keywords for text in normalized)


def detect_page_status(current_url: str, visible_texts: list[str]) -> str:
    normalized_url = (current_url or "").lower()

    if any(keyword in normalized_url for keyword in LOGIN_URL_KEYWORDS):
        return "登录页 - 请完成手机验证码登录"

    if _has_visible_keyword(visible_texts, PORTAL_SELECTION_KEYWORDS):
        return "入口选择页 - 请选择达人入口或店铺入口"

    if is_account_selection_page_snapshot(normalized_url, visible_texts):
        return "账号选择页 - 请选择要使用的账号"

    return f"等待中... (URL: {current_url[:80]})"


def is_account_selection_page_snapshot(current_url: str, visible_texts: list[str]) -> bool:
    normalized_url = (current_url or "").lower()

    if _has_visible_keyword(visible_texts, ACCOUNT_SELECTION_KEYWORDS):
        return True

    return (
        "compass.jinritemai.com" in normalized_url
        and any(keyword in normalized_url for keyword in ACCOUNT_SELECTION_PATH_KEYWORDS)
    )


def is_dashboard_page_snapshot(current_url: str, visible_texts: list[str]) -> bool:
    normalized_url = (current_url or "").lower()

    if any(keyword in normalized_url for keyword in LOGIN_URL_KEYWORDS):
        return False

    if is_account_selection_page_snapshot(normalized_url, visible_texts):
        return False

    is_compass_url = "compass.jinritemai.com" in normalized_url
    has_dashboard_path = any(keyword in normalized_url for keyword in DASHBOARD_PATH_KEYWORDS)

    normalized_visible = _normalize_visible_texts(visible_texts)
    nav_matches = sum(
        1 for keyword in DASHBOARD_NAV_TEXTS if any(keyword in text for text in normalized_visible)
    )
    has_nav = nav_matches >= 2

    if is_compass_url and has_dashboard_path:
        return True

    if is_compass_url and has_nav:
        return True

    return has_nav


def build_date_display_candidates(target_date: date) -> set[str]:
    return {
        target_date.strftime("%Y-%m-%d"),
        target_date.strftime("%Y/%m/%d"),
        f"{target_date.year}年{target_date.month}月{target_date.day}日",
        f"{target_date.month}月{target_date.day}日",
    }


def is_target_date_visible(target_date: date, haystacks: list[str]) -> bool:
    candidates = build_date_display_candidates(target_date)
    normalized = [text.strip() for text in haystacks if text and text.strip()]
    return any(candidate in haystack for candidate in candidates for haystack in normalized)
