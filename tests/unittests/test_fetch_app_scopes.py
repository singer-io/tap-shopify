import json
import unittest
import urllib.error
from unittest.mock import patch, MagicMock

import tap_shopify
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError, ShopifyUnauthorizedError


class TestFetchAppScopes(unittest.TestCase):
    """Tests for tap_shopify.fetch_app_scopes()."""

    def setUp(self):
        Context.client = None

    @patch('time.sleep', return_value=None)
    @patch('tap_shopify.get_request_timeout', return_value=300)
    @patch('shopify.GraphQL')
    def test_success_returns_scopes_and_passes_timeout(self, mock_graphql, mock_get_timeout, mock_sleep):
        """A successful call should return the set of granted scopes and pass an explicit timeout."""
        mock_response = {
            "data": {
                "currentAppInstallation": {
                    "accessScopes": [{"handle": "read_products"}, {"handle": "read_users"}]
                }
            }
        }
        mock_graphql.return_value.execute.return_value = json.dumps(mock_response)

        scopes = tap_shopify.fetch_app_scopes()

        self.assertEqual(scopes, {"read_products", "read_users"})
        _, kwargs = mock_graphql.return_value.execute.call_args
        self.assertEqual(kwargs.get('timeout'), 300)

    @patch('time.sleep', return_value=None)
    @patch('shopify.GraphQL')
    def test_401_http_error_raises_shopify_unauthorized_error(self, mock_graphql, mock_sleep):
        """A 401 HTTPError should be mapped to ShopifyUnauthorizedError."""
        mock_client = MagicMock()
        mock_client.refresh_token.return_value = False
        Context.client = mock_client

        http_error = urllib.error.HTTPError(
            url="https://test-shop.myshopify.com/admin/api/graphql.json",
            code=401,
            msg="Unauthorized",
            hdrs=MagicMock(**{"get.return_value": "req-401"}),
            fp=None,
        )
        mock_graphql.return_value.execute.side_effect = http_error

        with self.assertRaises(ShopifyUnauthorizedError):
            tap_shopify.fetch_app_scopes()

    @patch('time.sleep', return_value=None)
    @patch('shopify.GraphQL')
    def test_non_401_http_error_raises_shopify_error(self, mock_graphql, mock_sleep):
        """A non-401 HTTPError (e.g. 500) should be mapped to ShopifyError."""
        http_error = urllib.error.HTTPError(
            url="https://test-shop.myshopify.com/admin/api/graphql.json",
            code=500,
            msg="Internal Server Error",
            hdrs=MagicMock(**{"get.return_value": "req-500"}),
            fp=None,
        )
        mock_graphql.return_value.execute.side_effect = http_error

        with self.assertRaises(ShopifyError):
            tap_shopify.fetch_app_scopes()


if __name__ == '__main__':
    unittest.main()
