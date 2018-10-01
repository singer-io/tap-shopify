from unittest import TestCase
from tap_shopify import Metafields, Context
from singer import utils

class TestSubStreamBookmarkAccessors(TestCase):
    test_schema = None
    config = {"start_date": "2017-09-26T15:54:00.00000Z"}
    str_current_bookmark = "2018-09-26T15:54:00.00000Z"

    def setUp(self):
        Context.config = self.config
        Context.state = {}

    # Get Bookmark
    def test_get_bookmark_exists_root(self):
        instance = Metafields()

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"metafields": {"updated_at": current_bookmark}}}

        actual = instance.get_bookmark()
        self.assertEqual(utils.strptime_with_tz(current_bookmark), actual)

    def test_get_bookmark_does_not_exist_root(self):
        instance = Metafields()

        Context.state = {}

        actual = instance.get_bookmark()
        self.assertEqual(utils.strptime_with_tz(self.config["start_date"]), actual)

    def test_get_bookmark_exists_child(self):
        instance = Metafields(parent_type="orders")

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": current_bookmark}}}}

        actual = instance.get_bookmark()
        self.assertEqual(utils.strptime_with_tz(current_bookmark), actual)

    def test_get_bookmark_does_not_exist_child(self):
        instance = Metafields(parent_type="orders")

        Context.state = {"bookmarks": {"orders":{}}}

        actual = instance.get_bookmark()
        self.assertEqual(utils.strptime_with_tz(self.config["start_date"]), actual)

    def test_get_bookmark_parent_does_not_exist_child(self):
        instance = Metafields(parent_type="orders")

        Context.state = {"bookmarks": {}}

        actual = instance.get_bookmark()
        self.assertEqual(utils.strptime_with_tz(self.config["start_date"]), actual)

    # Write Bookmark
    def test_write_bookmark_exists_root(self):
        instance = Metafields()

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"metafields": {"updated_at": self.config["start_date"]}}}
        state_expected = dict(Context.state)
        state_expected["bookmarks"]["metafields"]["updated_at"] = current_bookmark

        instance.update_bookmark(current_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_does_not_exist_root(self):
        instance = Metafields()

        current_bookmark = self.str_current_bookmark
        Context.state = {}
        state_expected = {"bookmarks": {"metafields": {"updated_at": current_bookmark}}}

        instance.update_bookmark(current_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_exists_child(self):
        instance = Metafields("orders")

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": self.config["start_date"]}}}}
        state_expected = dict(Context.state)
        state_expected["bookmarks"]["orders"]["metafields"]["updated_at"] = current_bookmark

        instance.update_bookmark(current_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_does_not_exist_child(self):
        instance = Metafields("orders")

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {"orders": {}}}
        state_expected = {"bookmarks": { "orders": {"metafields": {"updated_at": current_bookmark}}}}

        instance.update_bookmark(current_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_parent_does_not_exist_child(self):
        instance = Metafields("orders")

        current_bookmark = self.str_current_bookmark
        Context.state = {"bookmarks": {}}
        state_expected = {"bookmarks": { "orders": {"metafields": {"updated_at": current_bookmark}}}}

        instance.update_bookmark(current_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_new_value_root(self):
        instance = Metafields()

        new_bookmark = self.config["start_date"]
        Context.state = {"bookmarks": {"metafields": {"updated_at": self.str_current_bookmark}}}
        state_expected = dict(Context.state)

        instance.update_bookmark(new_bookmark)
        self.assertEqual(state_expected, Context.state)

    def test_write_bookmark_new_value_child(self):
        instance = Metafields("orders")

        new_bookmark = self.config["start_date"]
        Context.state = {"bookmarks": {"orders": {"metafields": {"updated_at": self.str_current_bookmark}}}}
        state_expected = dict(Context.state)

        instance.update_bookmark(new_bookmark)
        self.assertEqual(state_expected, Context.state)
