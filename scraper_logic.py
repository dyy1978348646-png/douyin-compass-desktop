"""
纯逻辑判断，避免将页面识别规则硬编码在抓取主流程里。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

DATE_MODE_LAST_7_DAYS = "last_7_days"
DATE_MODE_LAST_1_DAY = "last_1_day"

SCENE_AUTO = "auto"
SCENE_UNKNOWN = "unknown"
SCENE_HOME_OVERVIEW = "home_overview"
SCENE_LIVE_REVIEW = "live_review"
SCENE_VIDEO_REVIEW = "video_review"
SCENE_SHOP_LIVE_DATA = "shop_live_data"

TASK_STATUS_PENDING = "pending"
TASK_STATUS_PRECHECKING = "prechecking"
TASK_STATUS_SELECTING_DATE = "selecting_date"
TASK_STATUS_EXPORTING = "exporting"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"

SCENE_DISPLAY_NAMES = {
    SCENE_AUTO: "按入口默认",
    SCENE_UNKNOWN: "未知场景",
    SCENE_HOME_OVERVIEW: "渠道数据",
    SCENE_LIVE_REVIEW: "直播数据",
    SCENE_VIDEO_REVIEW: "短视频数据",
    SCENE_SHOP_LIVE_DATA: "实时直播数据",
}

DATE_MODE_DISPLAY_NAMES = {
    DATE_MODE_LAST_7_DAYS: "近七天",
    DATE_MODE_LAST_1_DAY: "近一天",
}

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
HOME_OVERVIEW_KEYWORDS = ("首页", "整体概况", "整体概览", "更多", "近7天", "近30天", "自然周", "自然月", "大促")
LIVE_REVIEW_KEYWORDS = ("直播复盘", "直播")
VIDEO_REVIEW_KEYWORDS = ("视频复盘", "短视频")
VIDEO_REVIEW_DETAIL_KEYWORDS = ("视频复盘", "视频明细", "视频榜单", "视频表现", "更多数据")
HOME_EXTRA_KEYWORDS = ("核心数据", "流量来源")
SHOP_LIVE_DATA_KEYWORDS = ("实时直播数据",)
SCENE_VISIBLE_KEYWORDS = tuple(
    dict.fromkeys(
        DASHBOARD_NAV_TEXTS
        + HOME_OVERVIEW_KEYWORDS
        + LIVE_REVIEW_KEYWORDS
        + VIDEO_REVIEW_KEYWORDS
        + VIDEO_REVIEW_DETAIL_KEYWORDS
        + HOME_EXTRA_KEYWORDS
        + SHOP_LIVE_DATA_KEYWORDS
    )
)

ACCOUNT_NAME_EXACT_REJECTS = {
    "",
    "达人首页",
    "首页",
    "罗盘",
    "达人",
    "个人中心",
    "帮助",
    "退出登录",
    "切换账号",
    "切换子账号",
}
ACCOUNT_NAME_PARTIAL_REJECTS = (
    "退出登录",
    "切换账号",
    "切换子账号",
    "个人中心",
    "添加到桌面",
)


@dataclass(frozen=True)
class DateRangeSelection:
    mode: str
    start: date
    end: date
    label: str
    user_text: str = ""

    @property
    def is_single_day(self) -> bool:
        return self.start == self.end

    def to_payload(self) -> dict:
        return {
            "mode": self.mode,
            "label": self.label,
            "user_text": self.user_text,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "is_single_day": self.is_single_day,
        }


@dataclass(frozen=True)
class SceneDetection:
    scene_id: str
    confidence: str
    reason: str

    def to_payload(self) -> dict:
        return {
            "scene_id": self.scene_id,
            "scene_name": SCENE_DISPLAY_NAMES.get(self.scene_id, self.scene_id),
            "confidence": self.confidence,
            "reason": self.reason,
        }


def _normalize_visible_texts(visible_texts: list[str]) -> list[str]:
    return [text.strip() for text in visible_texts if text and text.strip()]


def _has_visible_keyword(visible_texts: list[str], keywords: tuple[str, ...]) -> bool:
    normalized = _normalize_visible_texts(visible_texts)
    return any(keyword in text for keyword in keywords for text in normalized)


def resolve_requested_scene(config: dict) -> str:
    configured = (config.get("scene_id") or SCENE_AUTO).strip()
    if configured in SCENE_DISPLAY_NAMES and configured not in {SCENE_AUTO, SCENE_UNKNOWN}:
        return configured

    if config.get("portal_type") == "shop":
        return SCENE_SHOP_LIVE_DATA

    return SCENE_LIVE_REVIEW


def resolve_target_date_range(config: dict, today: date | None = None) -> DateRangeSelection:
    today = today or datetime.now().date()
    mode = config.get("date_mode", DATE_MODE_LAST_7_DAYS)

    if mode == DATE_MODE_LAST_1_DAY:
        target = today - timedelta(days=1)
        return DateRangeSelection(
            mode=mode,
            start=target,
            end=target,
            label=DATE_MODE_DISPLAY_NAMES[mode],
        )

    start = today - timedelta(days=7)
    end = today - timedelta(days=1)
    return DateRangeSelection(
        mode=DATE_MODE_LAST_7_DAYS,
        start=start,
        end=end,
        label=DATE_MODE_DISPLAY_NAMES[DATE_MODE_LAST_7_DAYS],
    )


def detect_page_status(current_url: str, visible_texts: list[str]) -> str:
    normalized_url = (current_url or "").lower()

    if any(keyword in normalized_url for keyword in LOGIN_URL_KEYWORDS):
        return "登录页 - 请完成手机验证码登录"

    if _has_visible_keyword(visible_texts, PORTAL_SELECTION_KEYWORDS):
        return "入口选择页 - 请选择达人入口或店铺入口"

    if is_account_selection_page_snapshot(normalized_url, visible_texts):
        return "账号选择页 - 请选择要使用的账号"

    scene_detection = detect_scene_snapshot(current_url, visible_texts)
    if scene_detection.scene_id != SCENE_UNKNOWN and scene_detection.confidence in {"high", "medium"}:
        return f"已进入{SCENE_DISPLAY_NAMES[scene_detection.scene_id]}页面"

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


def detect_scene_snapshot(current_url: str, visible_texts: list[str]) -> SceneDetection:
    normalized_url = (current_url or "").lower()
    normalized_visible = _normalize_visible_texts(visible_texts)
    scores = {
        SCENE_HOME_OVERVIEW: 0,
        SCENE_LIVE_REVIEW: 0,
        SCENE_VIDEO_REVIEW: 0,
        SCENE_SHOP_LIVE_DATA: 0,
    }
    reasons = {scene_id: [] for scene_id in scores}

    def add_score(scene_id: str, score: int, reason: str):
        scores[scene_id] += score
        reasons[scene_id].append(reason)

    has_live_review_text = any("直播复盘" in text for text in normalized_visible)
    has_video_review_text = any("视频复盘" in text for text in normalized_visible)
    has_shop_realtime_text = any("实时直播数据" in text for text in normalized_visible)

    if has_live_review_text:
        add_score(SCENE_LIVE_REVIEW, 8, "页面文本命中“直播复盘”")

    if has_video_review_text:
        add_score(SCENE_VIDEO_REVIEW, 8, "页面文本命中“视频复盘”")

    if has_shop_realtime_text:
        add_score(SCENE_SHOP_LIVE_DATA, 4, "页面文本命中“实时直播数据”")

    has_home_overview_text = any(
        keyword in text for keyword in ("整体概况", "整体概览") for text in normalized_visible
    )
    has_home_more = any("更多" in text for text in normalized_visible)
    has_home_signature = any(
        keyword in text
        for keyword in ("自然周", "自然月", "大促")
        for text in normalized_visible
    )
    has_short_video_text = any("短视频" in text for text in normalized_visible)
    has_short_video_core = any("核心数据" in text for text in normalized_visible)
    has_live_traffic_source = any("流量来源" in text for text in normalized_visible)
    if any("实时直播数据" in text for text in normalized_visible):
        pass

    if has_home_overview_text and has_home_more and not (has_live_review_text or has_video_review_text):
        add_score(SCENE_HOME_OVERVIEW, 5, "页面文本命中“整体概况”卡片")
    elif has_home_more and has_home_signature and not (has_live_review_text or has_video_review_text):
        add_score(SCENE_HOME_OVERVIEW, 4, "页面文本命中首页筛选区特征")
    elif has_short_video_text and has_short_video_core and not (has_live_review_text or has_video_review_text):
        add_score(SCENE_HOME_OVERVIEW, 4, "页面文本命中短视频核心数据模块")
    elif has_live_traffic_source and has_short_video_text and not (has_live_review_text or has_video_review_text):
        add_score(SCENE_HOME_OVERVIEW, 3, "页面文本命中直播流量来源模块")

    if "realtime" in normalized_url:
        add_score(SCENE_SHOP_LIVE_DATA, 3, "URL 命中 realtime")
    if ("live" in normalized_url and "review" in normalized_url) or "replay" in normalized_url:
        add_score(SCENE_LIVE_REVIEW, 4, "URL 命中 live review")
    if (
        ("video" in normalized_url or "content" in normalized_url or "short" in normalized_url)
        and ("review" in normalized_url or "video-analysis" in normalized_url)
    ):
        add_score(SCENE_VIDEO_REVIEW, 4, "URL 命中视频复盘页面")
    if (
        "overview" in normalized_url
        or "dashboard" in normalized_url
        or normalized_url.rstrip("/").endswith("/home")
    ):
        add_score(SCENE_HOME_OVERVIEW, 2, "URL 命中首页概览")

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_scene, best_score = ranked[0]
    second_score = ranked[1][1]

    if best_score <= 0:
        return SceneDetection(SCENE_UNKNOWN, "unknown", "未识别到稳定场景特征")

    if best_score == second_score:
        return SceneDetection(SCENE_UNKNOWN, "low", "多个场景特征冲突，无法安全判断")

    confidence = "high" if best_score >= 6 else "medium" if best_score >= 3 else "low"
    return SceneDetection(best_scene, confidence, "；".join(reasons[best_scene]) or "命中场景规则")


def is_video_review_detail_page_snapshot(current_url: str, visible_texts: list[str]) -> bool:
    normalized_url = (current_url or "").lower()
    normalized_visible = _normalize_visible_texts(visible_texts)

    if "compass.jinritemai.com" not in normalized_url:
        return False

    has_video_analysis_url = "video-analysis" in normalized_url
    has_video_review_nav = any("视频复盘" in text for text in normalized_visible)
    has_detail_navigation = any(
        keyword in text for keyword in ("视频明细", "视频榜单") for text in normalized_visible
    )
    has_main_panel = any("视频表现" in text for text in normalized_visible)
    has_more_data = any("更多数据" in text for text in normalized_visible)

    return has_video_analysis_url and has_video_review_nav and has_main_panel and (
        has_more_data or has_detail_navigation
    )


def parse_metric_value(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    unit_multiplier = 1.0
    if "万" in text:
        unit_multiplier = 1e4
    elif "亿" in text:
        unit_multiplier = 1e8

    cleaned = re.sub(r"[¥￥,\s]", "", text)
    cleaned = cleaned.replace("万", "").replace("亿", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return float(match.group()) * unit_multiplier
    except ValueError:
        return None


def extract_metric_fragment(
    raw_text: str | None,
    labels: tuple[str, ...],
    regex_patterns: tuple[str, ...] = (),
) -> str | None:
    """从文本块中提取指标原始字符串。"""
    if not raw_text:
        return None

    lines = [line.strip() for line in str(raw_text).splitlines() if line and line.strip()]
    for index, line in enumerate(lines):
        for label in labels:
            if label not in line:
                continue
            same_line = line.replace(label, "").strip("：: ")
            if parse_metric_value(same_line) is not None:
                return same_line
            for offset in (1, 2):
                next_index = index + offset
                if next_index >= len(lines):
                    break
                candidate = lines[next_index].strip("：: ")
                if parse_metric_value(candidate) is not None:
                    return candidate

    text = str(raw_text)
    for pattern in regex_patterns:
        match = re.search(pattern, text, re.S)
        if not match:
            continue
        if match.groups():
            candidate = next((group for group in match.groups() if group), None)
            if candidate:
                return candidate.strip()
        matched_text = match.group(0).strip()
        if parse_metric_value(matched_text) is not None:
            return matched_text

    return None



def build_date_display_candidates(target_date: date) -> set[str]:
    return {
        target_date.strftime("%Y-%m-%d"),
        target_date.strftime("%Y/%m/%d"),
        f"{target_date.year}年{target_date.month}月{target_date.day}日",
        f"{target_date.month}月{target_date.day}日",
    }


def _normalize_date_haystack(text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    normalized = re.sub(r"\s*([\-~至到—])\s*", r"\1", normalized)
    return normalized


def normalize_chart_date_label(
    raw_label: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    text = _normalize_date_haystack(raw_label)
    if not text:
        return ""

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass

    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})", text)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        candidate_years = []
        if start_date:
            candidate_years.append(start_date.year)
        if end_date and end_date.year not in candidate_years:
            candidate_years.append(end_date.year)
        if not candidate_years:
            candidate_years.append(datetime.now().year)

        for year in candidate_years:
            try:
                candidate = date(year, month, day)
            except ValueError:
                continue
            if start_date and end_date and start_date <= candidate <= end_date:
                return candidate.isoformat()
            if not start_date or not end_date:
                return candidate.isoformat()

    return text


def build_video_review_export_rows(
    target_range: DateRangeSelection,
    tab_metrics: dict[str, dict[str, float | None]],
) -> list[dict]:
    start_iso = target_range.start.isoformat()
    end_iso = target_range.end.isoformat()
    rows: list[dict] = []

    for tab_name, metrics in (tab_metrics or {}).items():
        for label, value in (metrics or {}).items():
            rows.append(
                {
                    "数据类型": "summary",
                    "日期": start_iso if target_range.is_single_day else "",
                    "开始日期": start_iso,
                    "结束日期": end_iso,
                    "页签": tab_name,
                    "指标名称": label,
                    "指标值": value,
                }
            )

    return rows


def choose_account_name(candidates: list[str], fallback_title: str = "") -> str:
    def normalize(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip(" -|·")

    def is_valid(value: str) -> bool:
        if not value or len(value) < 2 or len(value) > 40:
            return False
        if value in ACCOUNT_NAME_EXACT_REJECTS:
            return False
        if any(keyword in value for keyword in ACCOUNT_NAME_PARTIAL_REJECTS):
            return False
        if "罗盘" in value and len(value) <= 10:
            return False
        return True

    for candidate in candidates or []:
        parts = re.split(r"[\r\n]+", str(candidate or ""))
        for part in parts:
            normalized = normalize(part)
            if is_valid(normalized):
                return normalized

    title = normalize(fallback_title)
    if title:
        for separator in (" - ", "的罗盘", "-", "—"):
            if separator in title:
                title = normalize(title.split(separator)[0])
                break
        if is_valid(title):
            return title

    return ""


def normalize_export_row_date(value) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return ""

    compact_match = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", text)
    if compact_match:
        year, month, day = compact_match.groups()
        return f"{year}-{month}-{day}"

    iso_match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
    if iso_match:
        year, month, day = iso_match.groups()
        return f"{year}-{month}-{day}"

    date_part_match = re.match(r"(\d{4})[-/](\d{2})[-/](\d{2})", text)
    if date_part_match:
        year, month, day = date_part_match.groups()
        return f"{year}-{month}-{day}"

    return ""


def is_target_date_visible(target_date: date, haystacks: list[str]) -> bool:
    candidates = build_date_display_candidates(target_date)
    normalized = [_normalize_date_haystack(text) for text in haystacks if text and text.strip()]
    return any(candidate in haystack for candidate in candidates for haystack in normalized)


def build_date_range_display_candidates(start_date: date, end_date: date) -> set[str]:
    candidates = set()

    if start_date == end_date:
        candidates.update(build_date_display_candidates(start_date))

    start_candidates = build_date_display_candidates(start_date)
    end_candidates = build_date_display_candidates(end_date)
    separators = (" - ", "-", " 至 ", "至", " ~ ", "~", " 到 ", "到", " — ", "—")

    for left in start_candidates:
        for right in end_candidates:
            for separator in separators:
                candidates.add(f"{left}{separator}{right}")

    return candidates


def is_target_date_range_visible(start_date: date, end_date: date, haystacks: list[str]) -> bool:
    normalized = [_normalize_date_haystack(text) for text in haystacks if text and text.strip()]

    if start_date == end_date and is_target_date_visible(start_date, normalized):
        return True

    start_candidates = build_date_display_candidates(start_date)
    end_candidates = build_date_display_candidates(end_date)

    for haystack in normalized:
        for start_candidate in start_candidates:
            for end_candidate in end_candidates:
                pattern = re.escape(start_candidate) + r"(?:-|~|至|到|—)" + re.escape(end_candidate)
                if re.search(pattern, haystack):
                    return True

    candidates = build_date_range_display_candidates(start_date, end_date)
    return any(_normalize_date_haystack(candidate) in haystack for candidate in candidates for haystack in normalized)
