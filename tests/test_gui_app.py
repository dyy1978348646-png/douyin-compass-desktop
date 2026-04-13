import unittest

from gui_app import (
    build_date_mode_options,
    build_scene_options,
    build_task_status_badge,
    compute_scroll_units,
    describe_scene_selection,
    resolve_runtime_config,
)


class ComputeScrollUnitsTests(unittest.TestCase):
    def test_linux_button4_scrolls_up(self):
        self.assertEqual(compute_scroll_units(num=4), -1)

    def test_linux_button5_scrolls_down(self):
        self.assertEqual(compute_scroll_units(num=5), 1)

    def test_windows_uses_120_step_delta(self):
        self.assertEqual(compute_scroll_units(delta=120, platform="windows"), -1)
        self.assertEqual(compute_scroll_units(delta=-240, platform="windows"), 2)

    def test_other_platform_uses_direction_only(self):
        self.assertEqual(compute_scroll_units(delta=7, platform="other"), -1)
        self.assertEqual(compute_scroll_units(delta=-3, platform="other"), 1)

    def test_zero_delta_does_not_scroll(self):
        self.assertEqual(compute_scroll_units(delta=0, platform="other"), 0)


class BuildSceneOptionsTests(unittest.TestCase):
    def test_creator_options_include_all_creator_scenes(self):
        values = [value for _, value in build_scene_options("creator")]
        self.assertEqual(values, ["auto", "home_overview", "live_review", "video_review"])

    def test_shop_options_only_include_shop_scenes(self):
        values = [value for _, value in build_scene_options("shop")]
        self.assertEqual(values, ["auto", "shop_live_data"])

    def test_auto_option_is_named_as_default_path(self):
        label, value = build_scene_options("creator")[0]
        self.assertEqual((label, value), ("按入口默认", "auto"))


class BuildDateModeOptionsTests(unittest.TestCase):
    def test_only_exposes_last_7_days_and_last_1_day(self):
        values = [value for _, value in build_date_mode_options()]
        self.assertEqual(values, ["last_7_days", "last_1_day"])


class DescribeSceneSelectionTests(unittest.TestCase):
    def test_auto_creator_scene_mentions_default_live_review(self):
        self.assertEqual(
            describe_scene_selection({"portal_type": "creator", "scene_id": "auto"}),
            "按入口默认（当前会走直播数据）",
        )

    def test_explicit_scene_uses_scene_name(self):
        self.assertEqual(
            describe_scene_selection({"portal_type": "creator", "scene_id": "home_overview"}),
            "渠道数据",
        )


class ResolveRuntimeConfigTests(unittest.TestCase):
    def test_saved_runtime_config_ignores_unsaved_form_values(self):
        saved_config = {
            "portal_type": "creator",
            "scene_id": "live_review",
            "date_mode": "last_7_days",
        }
        form_config = {
            "portal_type": "creator",
            "scene_id": "video_review",
            "date_mode": "last_1_day",
        }

        self.assertEqual(
            resolve_runtime_config(saved_config, form_config, use_saved_task=True),
            saved_config,
        )

    def test_manual_runtime_config_uses_current_form_values(self):
        saved_config = {
            "portal_type": "creator",
            "scene_id": "live_review",
            "date_mode": "last_7_days",
        }
        form_config = {
            "portal_type": "creator",
            "scene_id": "video_review",
            "date_mode": "last_1_day",
        }

        self.assertEqual(
            resolve_runtime_config(saved_config, form_config, use_saved_task=False),
            form_config,
        )


class BuildTaskStatusBadgeTests(unittest.TestCase):
    def test_success_status_maps_to_done_badge(self):
        self.assertEqual(build_task_status_badge("success"), ("已完成", "success"))

    def test_failed_status_maps_to_danger_badge(self):
        self.assertEqual(build_task_status_badge("failed"), ("执行失败", "danger"))


if __name__ == "__main__":
    unittest.main()
