import unittest
from unittest import mock
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
import datetime

from tap_shopify.streams.base import DATE_WINDOW_SIZE

CUSTOMER_OBJECT = Context.stream_objects['customers']()
Context.state = {}

class Customer():
    """The customer object to return in th api call"""
    def __init__(self, id, updated_at):
        self.id = id
        self.updated_at = updated_at

    def to_dict(self):
        return {"id": self.id, "updated_at": self.updated_at}


class DummyShopifyError(Exception):
    def __init__(self, error, msg=''):
        super().__init__('{}\n{}'.format(error.__class__.__name__, msg))


CUSTOMER_1 = Customer(20, "2021-08-11T01:57:05-04:00")
CUSTOMER_2 = Customer(30, "2021-08-12T01:57:05-04:00")

NOW_TIME = '2021-08-16T01:56:05-04:00'
class TestUpdateBookmark(unittest.TestCase):

    @mock.patch("tap_shopify.streams.base.Stream.call_api", return_value = [CUSTOMER_1, CUSTOMER_2])
    @mock.patch("tap_shopify.streams.base.Stream.get_bookmark", return_value=strptime_to_utc('2021-08-11T01:55:05-04:00'))
    @mock.patch("tap_shopify.streams.base.Stream.update_bookmark")
    @mock.patch("tap_shopify.streams.base.Stream.get_query_params")
    @mock.patch("singer.utils.now", return_value=strptime_to_utc(NOW_TIME))
    def test_update_bookmark(self, mock_now, mock_get_query_params, mock_update_bookmark, mock_get_bookmark, mock_call_api):
        """Verify that the update_bookmark() is called with correct argument"""
        list(CUSTOMER_OBJECT.get_objects())
        # Verify that the min-max evaluation returns correct results and provided in the update_bookmark()
        mock_update_bookmark.assert_called_with(strftime(strptime_to_utc(NOW_TIME) - datetime.timedelta(DATE_WINDOW_SIZE)))

    @mock.patch("tap_shopify.streams.base.Stream.call_api", return_value=[CUSTOMER_1, CUSTOMER_2])
    @mock.patch("singer.utils.now", return_value=strptime_to_utc(NOW_TIME))
    def test_since_id_and_updated_at_max_deleted(self, mock_now, mock_call_api):
        """Verify after successful sync since_id and updated_at_max keys are deleted from the state"""
        Context.state = {"bookmarks": {
            "currently_sync_stream": 'customers',
            "customers": {
                "updated_at": "2021-03-27T00:00:00.000000Z",
                "since_id": 15,
                "updated_at_max": "2021-04-26T00:00:00.000000Z"}}}

        list(CUSTOMER_OBJECT.get_objects())

        # Verify keys
        self.assertIn("updated_at", Context.state["bookmarks"]["customers"])
        self.assertNotIn("since_id", Context.state["bookmarks"]["customers"])
        self.assertNotIn("updated_at_max", Context.state["bookmarks"]["customers"])

    @mock.patch("tap_shopify.streams.base.Stream.call_api", side_effect=DummyShopifyError("Dummy Shopify exception..."))
    @mock.patch("singer.utils.now", return_value=strptime_to_utc(NOW_TIME))
    def test_interrupted_sync(self, mock_now, mock_call_api):
        """Verify if sync is interrrupted twice in a row then since_id and updated_at_max keys are not deleted from the state"""
        Context.state = {"bookmarks": {
            "currently_sync_stream": 'customers',
            "customers": {
                "updated_at": "2021-03-27T00:00:00.000000Z",
                "since_id": 15,
                "updated_at_max": "2021-04-26T00:00:00.000000Z"}}}

        with self.assertRaises(DummyShopifyError):
            list(CUSTOMER_OBJECT.get_objects())

        # Verify keys exist
        self.assertIn("since_id", Context.state["bookmarks"]["customers"])
        self.assertIn("updated_at_max", Context.state["bookmarks"]["customers"])

        # Verify bookmark key values are as expected
        self.assertEqual(Context.state["bookmarks"]["customers"]['since_id'], 15)
        self.assertEqual(Context.state["bookmarks"]["customers"]['updated_at_max'], "2021-04-26T00:00:00.000000Z")

