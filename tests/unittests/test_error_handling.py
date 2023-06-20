import unittest
from unittest import mock
from urllib.error import URLError
import tap_shopify
from tap_shopify.streams.transactions import Transactions
from tap_shopify.streams.orders import Orders
from tap_shopify.context import Context

class TestShopifyConnectionResetErrorHandling(unittest.TestCase):

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_check_access_handle_timeout_error(self, mocked_find, mocked_sleep):
        '''
        Test retry handling of URLError
        '''

        # mock 'find' and raise URLError
        mocked_find.side_effect = URLError('<urlopen error [Errno 104] Connection reset by peer>')

        # initialize class
        stream = Orders()

        with self.assertRaises(URLError):
            stream.call_api({})

        self.assertEqual(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_check_access_handle_connection_reset_error(self, mocked_find, mocked_sleep):
        '''
        Test retry handling of ConnectionResetError
        '''

        # mock 'find' and raise IncompleteRead error
        mocked_find.side_effect = ConnectionResetError

        # initialize class
        stream = Transactions()

        with self.assertRaises(ConnectionResetError):
            stream.replication_object.find()

        self.assertEqual(mocked_find.call_count, 5)
