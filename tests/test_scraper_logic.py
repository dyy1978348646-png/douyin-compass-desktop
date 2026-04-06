import unittest
from datetime import date

from scraper_logic import (
    build_date_display_candidates,
    detect_page_status,
    is_account_selection_page_snapshot,
    is_dashboard_page_snapshot,
    is_target_date_visible,
)


class PageStatusTests(unittest.TestCase):
    def test_detect_page_status_for_login_url(self):
        status = detect_page_status(
            current_url="https://sso.example.com/login",
            visible_texts=[],
        )
        self.assertEqual(status, "登录页 - 请完成手机验证码登录")

    def test_detect_page_status_for_portal_selection(self):
        status = detect_page_status(
            current_url="https://compass.jinritemai.com",
            visible_texts=["请选择达人入口"],
        )
        self.assertEqual(status, "入口选择页 - 请选择达人入口或店铺入口")

    def test_detect_page_status_for_account_selection(self):
        status = detect_page_status(
            current_url="https://compass.jinritemai.com/account/select",
            visible_texts=["选择账号"],
        )
        self.assertEqual(status, "账号选择页 - 请选择要使用的账号")


class DashboardDetectionTests(unittest.TestCase):
    def test_account_selection_snapshot_is_not_dashboard(self):
        self.assertTrue(
            is_account_selection_page_snapshot(
                current_url="https://compass.jinritemai.com/account/select",
                visible_texts=["选择账号"],
            )
        )
        self.assertFalse(
            is_dashboard_page_snapshot(
                current_url="https://compass.jinritemai.com/account/select",
                visible_texts=["选择账号", "直播", "商品"],
            )
        )

    def test_dashboard_url_with_navigation_is_dashboard(self):
        self.assertTrue(
            is_dashboard_page_snapshot(
                current_url="https://compass.jinritemai.com/talent/dashboard",
                visible_texts=["直播", "商品"],
            )
        )

    def test_dashboard_can_be_detected_by_navigation_texts_only(self):
        self.assertTrue(
            is_dashboard_page_snapshot(
                current_url="https://example.com/unknown",
                visible_texts=["直播", "短视频", "商品"],
            )
        )


class DateVisibilityTests(unittest.TestCase):
    def test_build_date_display_candidates(self):
        candidates = build_date_display_candidates(date(2026, 4, 6))
        self.assertIn("2026-04-06", candidates)
        self.assertIn("2026/04/06", candidates)
        self.assertIn("2026年4月6日", candidates)
        self.assertIn("4月6日", candidates)

    def test_is_target_date_visible(self):
        haystacks = ["2026-04-06", "近7天"]
        self.assertTrue(is_target_date_visible(date(2026, 4, 6), haystacks))
        self.assertFalse(is_target_date_visible(date(2026, 4, 7), haystacks))


if __name__ == "__main__":
    unittest.main()
