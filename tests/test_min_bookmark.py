from unittest import TestCase
from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields
from tap_shopify.streams.orders import Orders
from singer import utils

class TestMinBookmark(TestCase):
    test_schema = None
    config = {"start_date": "2017-09-26T15:54:00.00000Z"}
    str_current_bookmark = "2018-09-26T15:54:00.00000Z"

    def setUp(self):
        Context.config = self.config
        Context.state = {}
        Context.is_selected = lambda _: True

    def test_query_start_returns_bm_with_no_sub_streams(self):
        # A class with no substreams will just return the bm
        instance = Metafields()

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"metafields": {"updated_at": current_bookmark}}}

        actual = instance.query_start()
        self.assertEqual(utils.strptime_with_tz(current_bookmark), actual)

    def test_query_start_returns_min_bm_child_earlier(self):
        instance = Orders()

        # This will be less than the start_date
        current_bookmark = "2016-09-26T15:54:00.00000Z"
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": current_bookmark}}}}

        actual = instance.query_start()
        self.assertEqual(utils.strptime_with_tz(current_bookmark), actual)

    def test_query_start_returns_min_bm_child_not_earlier(self):
        instance = Orders()

        current_bookmark = "2018-09-26T15:54:00.00000Z"
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": current_bookmark}}}}

        actual = instance.query_start()
        self.assertEqual(utils.strptime_with_tz(self.config["start_date"]), actual)

    def test_query_start_returns_min_bm_child_with_lookback(self):
        Metafields.parent_lookback_window = 10
        instance = Orders()

        current_bookmark = "2016-09-26T15:54:00.00000Z"
        expected_value = "2016-09-16T15:54:00.00000Z"
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": current_bookmark}}}}

        actual = instance.query_start()
        self.assertEqual(utils.strptime_with_tz(expected_value), actual)
