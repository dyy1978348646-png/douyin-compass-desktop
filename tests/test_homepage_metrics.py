import unittest

from scraper_logic import (
    SCENE_HOME_OVERVIEW,
    detect_scene_snapshot,
    extract_metric_fragment,
    parse_metric_value,
)


class HomeOverviewDetectionTests(unittest.TestCase):
    def test_detects_home_overview_from_short_video_blocks(self):
        detection = detect_scene_snapshot(
            "https://compass.jinritemai.com/dashboard/home",
            ["首页", "短视频", "核心数据", "流量来源", "直播"],
        )
        self.assertEqual(detection.scene_id, SCENE_HOME_OVERVIEW)


class ParseMetricValueTests(unittest.TestCase):
    def test_parses_plain_number(self):
        self.assertEqual(parse_metric_value("123"), 123.0)

    def test_parses_number_with_commas(self):
        self.assertEqual(parse_metric_value("1,234"), 1234.0)

    def test_parses_currency(self):
        self.assertEqual(parse_metric_value("¥2,345.67"), 2345.67)

    def test_parses_wan_unit(self):
        self.assertEqual(parse_metric_value("1.2万"), 12000.0)

    def test_parses_yi_unit(self):
        self.assertEqual(parse_metric_value("3.4亿"), 340000000.0)

    def test_handles_empty(self):
        self.assertIsNone(parse_metric_value(""))


class ExtractMetricFragmentTests(unittest.TestCase):
    def test_extracts_value_from_label_line(self):
        block = "引流成交金额\n¥248,279.33\n较上周期\n24.18%"
        self.assertEqual(extract_metric_fragment(block, ("引流成交金额",)), "¥248,279.33")

    def test_extracts_video_review_leads_amounts(self):
        block = (
            "引流成交金额\n¥16,306.40\n较上周期↑103.06%\n"
            "引流直播间成交金额\n¥16,306.40\n较上周期↑111.46%"
        )
        self.assertEqual(extract_metric_fragment(block, ("引流成交金额",)), "¥16,306.40")
        self.assertEqual(
            extract_metric_fragment(block, ("引流直播间成交金额",)),
            "¥16,306.40",
        )

    def test_extracts_video_review_live_room_exposure(self):
        block = "引流直播间曝光次数\n4.66万\n较上周期↑17.74%"
        self.assertEqual(
            extract_metric_fragment(
                block,
                ("引流直播间曝光次数", "引流直播曝光次数", "直播间曝光次数"),
            ),
            "4.66万",
        )

    def test_extracts_live_review_summary_values(self):
        block = (
            "成交金额\n¥878,937.57\n"
            "直播场次\n9\n"
            "平台扶持流量\n6,881\n"
            "曝光-观看率（次数）\n4.29%"
        )
        self.assertEqual(extract_metric_fragment(block, ("成交金额",)), "¥878,937.57")
        self.assertEqual(extract_metric_fragment(block, ("直播场次",)), "9")
        self.assertEqual(extract_metric_fragment(block, ("平台扶持流量",)), "6,881")
        self.assertEqual(extract_metric_fragment(block, ("曝光-观看率",)), "4.29%")

    def test_extracts_value_from_sentence_pattern(self):
        block = "共有8.42万人次通过短视频引流成功下单（包含：直播间+橱窗+其他场景）"
        self.assertEqual(
            extract_metric_fragment(
                block,
                (),
                (r"共有([0-9.,]+(?:万|亿)?)人次通过短视频引流成功下单",),
            ),
            "8.42万",
        )

    def test_returns_none_when_metric_missing(self):
        self.assertIsNone(extract_metric_fragment("暂无数据", ("曝光次数",)))


if __name__ == "__main__":
    unittest.main()
