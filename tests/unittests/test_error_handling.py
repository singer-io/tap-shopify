import unittest
from unittest.mock import Mock
from urllib.error import URLError
import tap_shopify
from tap_shopify.context import Context


class TestShopifyConnectionResetErrorHandling(unittest.TestCase):

    def test_check_access_handle_timeout_error(self):
        '''
        Test retry handling of URLError
        '''

        stream = Context.stream_objects['orders']()
        stream.replication_object = Mock()
        stream.replication_object.find = Mock()
        stream.replication_object.find.side_effect = URLError('<urlopen error [Errno 104] Connection reset by peer>')

        with self.assertRaises(URLError):
            stream.call_api({})

        self.assertEqual(stream.replication_object.find.call_count, 5)
