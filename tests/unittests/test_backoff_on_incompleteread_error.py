from unittest import mock
from tap_shopify.streams.base import http
from tap_shopify.streams.transactions import Transactions
import unittest

class TestIncompleteReadBackoff(unittest.TestCase):
    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_Transactions_pyactiveresource_error_incomplete_read_error_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'http.client.IncompleteRead' error occurs
        """
        # mock 'find' and raise IncompleteRead error
        mocked_find.side_effect = http.client.IncompleteRead(b'')
        # initialize class
        locations = Transactions()
        try:
            # function call
            locations.replication_object.find()
        except http.client.IncompleteRead:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)