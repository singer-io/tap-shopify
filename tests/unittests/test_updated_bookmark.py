import unittest
from unittest import mock
from singer.utils import strptime_with_tz, strftime, strptime_to_utc
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


CUSTOMER_1 = Customer(2, "2021-08-11T01:57:05-04:00")
CUSTOMER_2 = Customer(3, "2021-08-12T01:57:05-04:00")

NOW_TIME = '2021-08-16T01:56:05-04:00'
class TestUpdateBookmark(unittest.TestCase):

    @mock.patch("tap_shopify.streams.base.Stream.call_api")
    @mock.patch("tap_shopify.streams.base.Stream.get_bookmark", return_value=strptime_with_tz('2021-08-11T01:55:05-04:00'))
    @mock.patch("tap_shopify.streams.base.Stream.update_bookmark")
    @mock.patch("tap_shopify.streams.base.Stream.get_query_params")
    @mock.patch("singer.utils.now", return_value=strptime_to_utc(NOW_TIME))
    def test_update_bookmark(self, mock_now, mock_get_query_params, mock_update_bookmark, mock_get_bookmark, mock_call_api):
        """Verify that the update_bookmark() is called with correct argument"""
        mock_call_api.return_value = [CUSTOMER_1, CUSTOMER_2]

        customers = list(CUSTOMER_OBJECT.get_objects())
        # Verify that the min-max evaluation returns correct results and provided in the update_bookmark()
        mock_update_bookmark.assert_called_with(strftime(strptime_to_utc(NOW_TIME) - datetime.timedelta(DATE_WINDOW_SIZE)))
