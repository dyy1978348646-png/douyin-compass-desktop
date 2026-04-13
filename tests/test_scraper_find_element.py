import unittest

from scraper import DouyinCompassScraper


class DummyElement:
    def __init__(self, visible=True):
        self._visible = visible

    def is_visible(self):
        return self._visible


class DummyLocator:
    def __init__(self, visible=False):
        self._visible = visible

    def is_visible(self, timeout=None):
        return self._visible


class DummyLocatorWrapper:
    def __init__(self, locator):
        self._locator = locator or DummyLocator(False)

    @property
    def first(self):
        return self._locator


class DummyTarget:
    def __init__(self, elements=None, locators=None, frames=None):
        self._elements = elements or {}
        self._locators = locators or {}
        self.frames = frames or []

    def query_selector(self, selector):
        return self._elements.get(selector)

    def locator(self, selector):
        locator = self._locators.get(selector)
        return DummyLocatorWrapper(locator)


class DummyRoleLocator:
    def __init__(self, visible=False):
        self._visible = visible

    @property
    def first(self):
        return self

    def is_visible(self, timeout=None):
        return self._visible


class DummyRoleTarget(DummyTarget):
    def __init__(self, *args, roles=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._roles = roles or {}

    def get_by_role(self, role, name=None):
        return self._roles.get((role, name), DummyRoleLocator(False))


class DelayedTarget(DummyTarget):
    def __init__(self, *args, appear_after=2, **kwargs):
        super().__init__(*args, **kwargs)
        self._appear_after = appear_after
        self._calls = 0

    def query_selector(self, selector):
        self._calls += 1
        if self._calls >= self._appear_after:
            return self._elements.get(selector)
        return None


class DummyPageUrl:
    def __init__(self, url=""):
        self.url = url


class FindElementTests(unittest.TestCase):
    def test_searches_frames_when_root_has_no_match(self):
        selector = 'span:has-text("近1天")'
        child_element = DummyElement(True)
        child = DummyTarget(elements={selector: child_element})
        root = DummyTarget(frames=[child])

        scraper = DouyinCompassScraper({})
        scraper.page = root

        found = scraper._find_element([selector], "近一天按钮", target=root, max_attempts=1, wait_ms=0)
        self.assertIs(found, child_element)

    def test_retries_until_element_appears(self):
        selector = 'span:has-text("近1天")'
        element = DummyElement(True)
        target = DelayedTarget(elements={selector: element}, appear_after=2)

        scraper = DouyinCompassScraper({})
        scraper.page = target

        found = scraper._find_element([selector], "近一天按钮", target=target, max_attempts=2, wait_ms=0)
        self.assertIs(found, element)

    def test_find_element_by_role_searches_frames(self):
        locator = DummyRoleLocator(True)
        child = DummyRoleTarget(roles={("tab", "近1天"): locator})
        root = DummyRoleTarget(frames=[child])

        scraper = DouyinCompassScraper({})
        scraper.page = root

        found = scraper._find_element_by_role(["近1天"], target=root)
        self.assertIs(found, locator)


class VideoReviewEntryUrlTests(unittest.TestCase):
    def test_builds_video_review_entry_urls_from_current_origin(self):
        scraper = DouyinCompassScraper({})
        scraper.page = DummyPageUrl("https://compass.jinritemai.com/talent")

        urls = scraper._build_video_review_entry_urls()

        self.assertEqual(
            urls,
            [
                "https://compass.jinritemai.com/talent/video-analysis",
                "https://compass.jinritemai.com/talent/video-analysis?from_page=%2Ftalent",
            ],
        )

    def test_builds_video_review_entry_urls_from_config_when_page_empty(self):
        scraper = DouyinCompassScraper({"compass_url": "https://compass.jinritemai.com"})
        scraper.page = DummyPageUrl("")

        urls = scraper._build_video_review_entry_urls()

        self.assertEqual(urls[0], "https://compass.jinritemai.com/talent/video-analysis")


if __name__ == "__main__":
    unittest.main()
