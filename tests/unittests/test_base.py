import json
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dateutil.tz import tzlocal
from itertools import cycle
from tap_shopify.streams.base import Stream, ShopifyAPIError
from tap_shopify.context import Context

class TestStream(unittest.TestCase):

    def mock_query():
        return """
            {
                products(first: 10, after: "cursor") {
                    edges {
                        node {
                            id
                            updatedAt
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
            """
    def setUp(self):
        self.stream = Stream()
        self.stream.data_key = "products"
        self.stream.name = "products"
        # Mock the Context.config to include start_date
        self.original_config = Context.config
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": 30
        }
        Context.catalog = {
            "streams": [
                {
                    "tap_stream_id": "products",
                    "schema": {
                        "properties": {
                            "id": {"type": "string"},
                            "updatedAt": {"type": "string"}
                        }
                    },
                    "metadata": []
                }
            ]
        }

    def tearDown(self):
        # Reset Context.config to its original state
        Context.config = self.original_config

    @patch('shopify.GraphQL')
    @patch.object(Stream, 'get_query', return_value='mocked_query')
    def test_call_api_success(self, mock_get_query, mock_graphql):
        """Test successful GraphQL query execution."""
        # Mock the response from Shopify GraphQL API
        mock_response = {
            "data": {
                "products": {
                    "edges": [{"node": {"id": "mocked_id", "updated_at": "2025-01-01T00:00:00Z"}}],
                    "pageInfo": {"endCursor": "cursor_123", "hasNextPage": False},
                }
            }
        }
        mock_graphql.return_value.execute.return_value = json.dumps(mock_response)

        query_params = self.stream.get_query_params("2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z")
        result = self.stream.call_api(query_params)

        self.assertEqual(result, mock_response["data"]["products"])

    @patch('shopify.GraphQL')
    @patch.object(Stream, 'get_query', return_value='mocked_query')
    def test_call_api_error(self, mock_get_query, mock_graphql):
        """Test GraphQL query execution with an error."""
        # Mock an error response from Shopify GraphQL API
        mock_graphql.return_value.execute.side_effect = ShopifyAPIError("GraphQL error")

        query_params = self.stream.get_query_params("2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z")

        with self.assertRaises(ShopifyAPIError):
            self.stream.call_api(query_params)

    @patch('shopify.GraphQL')
    @patch.object(Stream, 'get_query', return_value='mocked_query')
    def test_call_api_empty_response(self, mock_get_query, mock_graphql):
        """Test GraphQL query execution with an empty response."""
        # Mock an empty response from Shopify GraphQL API
        mock_response = {}
        mock_graphql.return_value.execute.return_value = json.dumps(mock_response)

        query_params = self.stream.get_query_params("2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z")
        result = self.stream.call_api(query_params)

        self.assertEqual(result, {})

    @patch('shopify.GraphQL')
    @patch.object(Stream, 'get_query', return_value=mock_query())
    @patch.object(Stream, 'transform_object', side_effect=lambda x, **_kwargs: x)
    @patch('tap_shopify.streams.base.utils.now', return_value=datetime(2025, 2, 1, 0, 0, tzinfo=tzlocal()))
    def test_get_objects(self, mock_now, mock_transform_object, mock_get_query, mock_graphql):
        """Test get_objects with pagination and bookmarking."""
        # Mock the response from Shopify GraphQL API
        mock_response_page_1 = {
            "data": {
                "products": {
                    "edges": [{"node": {"id": "mocked_id_1", "updatedAt": "2025-01-01T00:00:00Z"}}],
                    "pageInfo": {"endCursor": "cursor_123", "hasNextPage": True},
                }
            }
        }
        mock_response_page_2 = {
            "data": {
                "products": {
                    "edges": [{"node": {"id": "mocked_id_2", "updatedAt": "2025-01-01T00:00:00Z"}}],
                    "pageInfo": {"endCursor": "cursor_456", "hasNextPage": False},
                }
            }
        }
        alternating_responses = cycle([
            json.dumps(mock_response_page_1),
            json.dumps(mock_response_page_2)
        ])

        # Set side_effect to use the infinite alternating cycle
        mock_graphql.return_value.execute.side_effect = lambda *args, **kwargs: next(alternating_responses)

        objects = list(self.stream.get_objects())

        self.assertEqual(len(objects), 4)
        self.assertEqual(objects[0], {"id": "mocked_id_1", "updatedAt": "2025-01-01T00:00:00Z"})
        self.assertEqual(objects[1], {"id": "mocked_id_2", "updatedAt": "2025-01-01T00:00:00Z"})
