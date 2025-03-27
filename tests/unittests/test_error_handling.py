import unittest
import http.client
import socket
from parameterized import parameterized
import pyactiveresource
import simplejson
from urllib.error import URLError
from unittest.mock import patch, MagicMock
import itertools
from tap_shopify.streams.products import Products
from tap_shopify.streams.base import ShopifyAPIError

class MockResponse:
    def __init__(self, msg, url, code):
        self.msg = msg
        self.url = url
        self.code = code

class TestShopifyErrorHandling(unittest.TestCase):

    @parameterized.expand([
        ["http_incompleteread_error", lambda: http.client.IncompleteRead(10), http.client.IncompleteRead],
        ["connection_reset_error", lambda: ConnectionResetError("Connection reset by peer"), ConnectionResetError],
        ["shopify_api_error", lambda: ShopifyAPIError("Shopify API error"), ShopifyAPIError],
        ["pyactiveresource_connection_error", lambda: pyactiveresource.connection.Error("Resource connection error with timed out"), pyactiveresource.connection.Error],
        ["socket_timeout_error", lambda: socket.timeout("The read operation timed out"), TimeoutError],
        ["server_error", lambda: pyactiveresource.connection.ServerError(MockResponse("Server error", "https://shopify.com", 500)), pyactiveresource.connection.ServerError],
        ["formats_error", lambda: pyactiveresource.formats.Error("Format error"), pyactiveresource.formats.Error],
        ["json_decode_error", lambda: simplejson.scanner.JSONDecodeError("JSON decode error", "doc", 0), simplejson.scanner.JSONDecodeError],
        ["url_error", lambda: URLError("URL error"), URLError]
    ])
    @patch("shopify.GraphQL")
    def test_api_errors(self, name, error_fn, expected_exception, mock_graphql):
        """Test handling of different API errors"""

        side_effect = error_fn()  # Call the lambda to create the exception instance

        with self.subTest(name=name, side_effect=side_effect, expected_exception=expected_exception):
            # Mock GraphQL execute method to simulate errors
            mock_graphql_instance = mock_graphql.return_value
            mock_graphql_instance.execute = MagicMock(side_effect=itertools.repeat(side_effect, 5))

            obj = Products()
            obj.data_key = "products"

            with self.assertRaises(expected_exception):
                obj.call_api(query_params={})

            self.assertEqual(mock_graphql_instance.execute.call_count, 5)
