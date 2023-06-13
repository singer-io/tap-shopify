import unittest
from unittest import mock
from urllib.error import URLError
import tap_shopify
from tap_shopify.streams.transactions import Transactions
from tap_shopify.context import Context
from parameterized import parameterized

class TestShopifyConnectionResetErrorHandling(unittest.TestCase):

    @parameterized.expand([
       (URLError, URLError('<urlopen error [Errno 104] Connection reset by peer>'), 'orders', 'call_api({})', 5),
       (ConnectionResetError, ConnectionResetError, 'transactions', 'replication_object.find()', 5),
    ])
    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_check_access(self, error_type, error, stream_name, func, expected_retries, mocked_find, mocked_sleep):
        '''
        Test retry handling of URLError and ConnectionResetError
        '''

        # mock 'find' and raise appropriate error
        mocked_find.side_effect = error

        #initialize class
        stream = Context.stream_objects[stream_name]()
        full_function = 'stream.' + func

        with self.assertRaises(error_type):
           exec(full_function)

        # verify we backoff expected number of times
        self.assertEqual(mocked_find.call_count, expected_retries)
