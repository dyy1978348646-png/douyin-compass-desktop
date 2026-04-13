import unittest
from datetime import date

from scraper_logic import (
    DATE_MODE_LAST_1_DAY,
    DATE_MODE_LAST_7_DAYS,
    SCENE_HOME_OVERVIEW,
    SCENE_LIVE_REVIEW,
    SCENE_SHOP_LIVE_DATA,
    SCENE_UNKNOWN,
    SCENE_VIDEO_REVIEW,
    DateRangeSelection,
    build_video_review_export_rows,
    choose_account_name,
    detect_scene_snapshot,
    is_video_review_detail_page_snapshot,
    is_target_date_range_visible,
    normalize_chart_date_label,
    normalize_export_row_date,
    resolve_requested_scene,
    resolve_target_date_range,
)


class ResolveTargetDateRangeTests(unittest.TestCase):
    def test_last_1_day_uses_previous_natural_day(self):
        selection = resolve_target_date_range(
            {"date_mode": DATE_MODE_LAST_1_DAY},
            today=date(2026, 4, 8),
        )
        self.assertEqual(selection.start, date(2026, 4, 7))
        self.assertEqual(selection.end, date(2026, 4, 7))

    def test_last_7_days_excludes_today(self):
        selection = resolve_target_date_range(
            {"date_mode": DATE_MODE_LAST_7_DAYS},
            today=date(2026, 4, 8),
        )
        self.assertEqual(selection.start, date(2026, 4, 1))
        self.assertEqual(selection.end, date(2026, 4, 7))

    def test_legacy_custom_date_mode_falls_back_to_last_7_days(self):
        selection = resolve_target_date_range(
            {"date_mode": "custom_date", "custom_date_text": "2026年4月7日"},
            today=date(2026, 4, 8),
        )
        self.assertEqual(selection.mode, DATE_MODE_LAST_7_DAYS)
        self.assertEqual(selection.start, date(2026, 4, 1))
        self.assertEqual(selection.end, date(2026, 4, 7))


class ResolveRequestedSceneTests(unittest.TestCase):
    def test_creator_defaults_to_live_review(self):
        self.assertEqual(
            resolve_requested_scene({"portal_type": "creator", "scene_id": "auto"}),
            SCENE_LIVE_REVIEW,
        )

    def test_shop_defaults_to_shop_live_data(self):
        self.assertEqual(
            resolve_requested_scene({"portal_type": "shop", "scene_id": "auto"}),
            SCENE_SHOP_LIVE_DATA,
        )

    def test_explicit_scene_wins(self):
        self.assertEqual(
            resolve_requested_scene({"portal_type": "creator", "scene_id": SCENE_VIDEO_REVIEW}),
            SCENE_VIDEO_REVIEW,
        )


class DetectSceneSnapshotTests(unittest.TestCase):
    def test_detects_live_review_from_visible_text(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/live/review",
            ["直播", "直播复盘", "自定义"],
        )
        self.assertEqual(detection.scene_id, SCENE_LIVE_REVIEW)

    def test_detects_video_review_from_visible_text(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/content/video/review",
            ["短视频", "视频复盘", "近7天"],
        )
        self.assertEqual(detection.scene_id, SCENE_VIDEO_REVIEW)

    def test_detects_video_review_from_video_analysis_url(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/talent/video-analysis?from_page=%2Ftalent",
            ["短视频", "视频复盘", "视频表现", "更多数据"],
        )
        self.assertEqual(detection.scene_id, SCENE_VIDEO_REVIEW)

    def test_detects_home_overview_from_home_filters(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/dashboard/home",
            ["首页", "更多", "近7天", "近30天"],
        )
        self.assertEqual(detection.scene_id, SCENE_HOME_OVERVIEW)

    def test_detects_home_overview_from_overall_overview_card(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/dashboard/home",
            ["首页", "整体概况", "更多", "近7天"],
        )
        self.assertEqual(detection.scene_id, SCENE_HOME_OVERVIEW)

    def test_video_review_outweighs_global_realtime_menu_text(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/content/video/review",
            ["短视频", "视频复盘", "实时直播数据"],
        )
        self.assertEqual(detection.scene_id, SCENE_VIDEO_REVIEW)

    def test_live_review_outweighs_home_filter_texts(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/live/review",
            ["首页", "更多", "自然周", "自然月", "大促", "直播复盘"],
        )
        self.assertEqual(detection.scene_id, SCENE_LIVE_REVIEW)

    def test_returns_unknown_when_signals_conflict(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/ambiguous",
            ["直播复盘", "视频复盘"],
        )
        self.assertEqual(detection.scene_id, SCENE_UNKNOWN)


class VideoReviewDetailPageSnapshotTests(unittest.TestCase):
    def test_accepts_video_review_detail_page(self):
        self.assertTrue(
            is_video_review_detail_page_snapshot(
                "https://compass.jinritemai.com/talent/video-analysis?from_page=%2Ftalent",
                ["短视频", "视频复盘", "视频明细", "视频榜单", "视频表现", "更多数据"],
            )
        )

    def test_accepts_video_review_detail_page_when_more_data_visible(self):
        self.assertTrue(
            is_video_review_detail_page_snapshot(
                "https://compass.jinritemai.com/talent/video-analysis?from_page=%2Ftalent",
                ["短视频", "视频复盘", "视频表现", "更多数据"],
            )
        )

    def test_rejects_homepage_short_video_block(self):
        self.assertFalse(
            is_video_review_detail_page_snapshot(
                "https://compass.jinritemai.com/talent",
                ["首页", "短视频", "视频复盘", "核心数据", "流量来源", "更多"],
            )
        )


class TargetDateRangeVisibleTests(unittest.TestCase):
    def test_single_day_range_accepts_date_range_display(self):
        self.assertTrue(
            is_target_date_range_visible(
                date(2026, 4, 7),
                date(2026, 4, 7),
                ["2026/04/07 - 2026/04/07"],
            )
        )

    def test_multi_day_range_accepts_range_display(self):
        self.assertTrue(
            is_target_date_range_visible(
                date(2026, 4, 1),
                date(2026, 4, 7),
                ["2026-04-01 至 2026-04-07"],
            )
        )

    def test_multi_day_range_accepts_extra_spaces(self):
        self.assertTrue(
            is_target_date_range_visible(
                date(2026, 4, 4),
                date(2026, 4, 10),
                ["2026/04/04  -  2026/04/10"],
            )
        )


class NormalizeChartDateLabelTests(unittest.TestCase):
    def test_keeps_full_iso_date(self):
        self.assertEqual(
            normalize_chart_date_label("2026/04/10", end_date=date(2026, 4, 10)),
            "2026-04-10",
        )

    def test_resolves_short_month_day_within_range(self):
        self.assertEqual(
            normalize_chart_date_label(
                "04/04",
                start_date=date(2026, 4, 4),
                end_date=date(2026, 4, 10),
            ),
            "2026-04-04",
        )


class BuildVideoReviewExportRowsTests(unittest.TestCase):
    def test_builds_single_day_summary_rows_per_metric(self):
        rows = build_video_review_export_rows(
            DateRangeSelection(
                mode=DATE_MODE_LAST_1_DAY,
                start=date(2026, 4, 10),
                end=date(2026, 4, 10),
                label="近一天",
            ),
            {
                "引流价值": {
                    "引流成交金额": 100.0,
                    "引流直播间成交金额": 80.0,
                    "引流直播间曝光次数": 3000.0,
                },
                "直接成交": {
                    "结算有效成交金额": 317.0,
                },
            },
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["数据类型"], "summary")
        self.assertEqual(rows[0]["日期"], "2026-04-10")
        self.assertEqual(rows[0]["开始日期"], "2026-04-10")
        self.assertEqual(rows[0]["结束日期"], "2026-04-10")
        self.assertEqual(rows[0]["页签"], "引流价值")
        self.assertEqual(rows[0]["指标名称"], "引流成交金额")
        self.assertEqual(rows[0]["指标值"], 100.0)
        self.assertEqual(rows[-1]["页签"], "直接成交")
        self.assertEqual(rows[-1]["指标名称"], "结算有效成交金额")
        self.assertEqual(rows[-1]["指标值"], 317.0)

    def test_builds_summary_rows_per_metric_for_last_7_days(self):
        rows = build_video_review_export_rows(
            DateRangeSelection(
                mode=DATE_MODE_LAST_7_DAYS,
                start=date(2026, 4, 4),
                end=date(2026, 4, 10),
                label="近七天",
            ),
            {
                "引流价值": {
                    "引流成交金额": 700.0,
                },
                "直接成交": {
                    "结算有效成交订单量": 10.0,
                },
            },
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["数据类型"], "summary")
        self.assertEqual(rows[0]["日期"], "")
        self.assertEqual(rows[0]["开始日期"], "2026-04-04")
        self.assertEqual(rows[0]["结束日期"], "2026-04-10")
        self.assertEqual(rows[0]["页签"], "引流价值")
        self.assertEqual(rows[0]["指标名称"], "引流成交金额")
        self.assertEqual(rows[0]["指标值"], 700.0)
        self.assertEqual(rows[1]["页签"], "直接成交")
        self.assertEqual(rows[1]["指标名称"], "结算有效成交订单量")
        self.assertEqual(rows[1]["指标值"], 10.0)


class NormalizeExportRowDateTests(unittest.TestCase):
    def test_normalizes_compact_yyyymmdd(self):
        self.assertEqual(normalize_export_row_date("20260412"), "2026-04-12")

    def test_normalizes_integer_yyyymmdd(self):
        self.assertEqual(normalize_export_row_date(20260412), "2026-04-12")

    def test_normalizes_iso_text(self):
        self.assertEqual(normalize_export_row_date("2026-04-12"), "2026-04-12")

    def test_returns_empty_for_unsupported_value(self):
        self.assertEqual(normalize_export_row_date("not-a-date"), "")


class ChooseAccountNameTests(unittest.TestCase):
    def test_prefers_real_account_name_over_generic_dashboard_text(self):
        self.assertEqual(
            choose_account_name(["达人首页", "小米耳机官方直播间", "帮助"]),
            "小米耳机官方直播间",
        )

    def test_extracts_account_name_from_dropdown_lines(self):
        self.assertEqual(
            choose_account_name(["小米耳机官方直播间\n切换子账号\n退出登录"]),
            "小米耳机官方直播间",
        )

    def test_falls_back_to_title_only_when_title_is_specific(self):
        self.assertEqual(
            choose_account_name([], fallback_title="小米耳机官方直播间 - 抖音电商罗盘"),
            "小米耳机官方直播间",
        )

    def test_rejects_generic_title_fallback(self):
        self.assertEqual(
            choose_account_name([], fallback_title="达人首页-抖音电商罗盘"),
            "",
        )


if __name__ == "__main__":
    unittest.main()
