import datetime
import unittest
from tap_shopify.context import Context
from unittest.mock import patch, MagicMock

import requests
from tap_shopify.streams.graphql_base import GraphQLStream, ShopifyGraphQLError


class TestGraphQLStream(unittest.TestCase):
    def setUp(self):
        # Create a subclass of GraphQLStream to test the abstract methods
        class TestGraphQLStream(GraphQLStream):
            Context.config = {'shop': 'test.myshopify.com', 'api_key': 'test_key'}

            def get_graphql_query(self):
                return "query { test }"

            def get_connection_from_data(self, data):
                return data['data']['testConnection']

            def process_node(self, node):
                return node

        self.stream = TestGraphQLStream()

    @patch.object(GraphQLStream, 'query')
    @patch.object(GraphQLStream, 'update_bookmark')
    def test_sync(self, mock_update_bookmark, mock_query):
        # Mock data returned by the query
        mock_query.side_effect = [
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'}},
                            {'node': {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'cursor1'
                        }
                    }
                }
            },
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': False,
                            'endCursor': None
                        }
                    }
                }
            }
        ]

        # Mock the replication key and initial bookmark
        self.stream.replication_key = 'updatedAt'
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Collect the results from the sync method
        results = list(self.stream.sync())

        # Verify the results
        expected_results = [
            {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'},
            {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'},
            {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}
        ]
        self.assertEqual(results, expected_results)

        # Verify the bookmark was updated correctly
        mock_update_bookmark.assert_called_with('2023-01-03T00:00:00Z')

    @patch.object(GraphQLStream, 'query')
    def test_get_objects(self, mock_query):
        # Mock data returned by the query
        mock_query.side_effect = [
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'}},
                            {'node': {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'cursor1'
                        }
                    }
                }
            },
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': False,
                            'endCursor': None
                        }
                    }
                }
            }
        ]

        # Mock the replication key and initial bookmark
        self.stream.replication_key = 'updatedAt'
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Collect the results from the get_objects method
        results = list(self.stream.get_objects())

        # Verify the results
        expected_results = [
            {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'},
            {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'},
            {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}
        ]
        self.assertEqual(results, expected_results)

    def test_get_bookmark(self):
        # Mock the bookmark value
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Verify the bookmark value
        self.assertEqual(self.stream.get_bookmark(), datetime.datetime(2022, 12, 31))

    def test_update_bookmark(self):
        # Mock the update_bookmark method
        self.stream.update_bookmark = MagicMock()

        # Call the update_bookmark method
        self.stream.update_bookmark('2023-01-03T00:00:00Z')

        # Verify the update_bookmark method was called with the correct arguments
        self.stream.update_bookmark.assert_called_with('2023-01-03T00:00:00Z')

    @patch('requests.post')
    def test_query_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': {'test': 'value'}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        result = self.stream.query('query { test }')
        self.assertEqual(result, {'test': 'value'})

    @patch('requests.post')
    def test_query_with_errors(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'errors': ['error']}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        with self.assertRaises(ShopifyGraphQLError):
            self.stream.query('query { test }')

    @patch('requests.post')
    def test_query_http_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_post.return_value = mock_response

        with self.assertRaises(requests.exceptions.HTTPError):
            self.stream.query('query { test }')

    @patch('requests.post')
    def test_query_connection_error(self, mock_post):
        mock_post.side_effect = requests.exceptions.ConnectionError()

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.stream.query('query { test }')

    @patch('requests.post')
    def test_query_timeout(self, mock_post):
        mock_post.side_effect = requests.exceptions.Timeout()

        with self.assertRaises(requests.exceptions.Timeout):
            self.stream.query('query { test }')

    @patch('requests.post')
    def test_query_rate_limit(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=MagicMock(status_code=429))
        mock_post.return_value = mock_response

        with self.assertRaises(requests.exceptions.HTTPError):
            self.stream.query('query { test }')

        self.stream = TestGraphQLStream()

    @patch.object(GraphQLStream, 'query')
    @patch.object(GraphQLStream, 'update_bookmark')
    def test_sync(self, mock_update_bookmark, mock_query):
        # Mock data returned by the query
        mock_query.side_effect = [
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'}},
                            {'node': {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'cursor1'
                        }
                    }
                }
            },
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': False,
                            'endCursor': None
                        }
                    }
                }
            }
        ]

        # Mock the replication key and initial bookmark
        self.stream.replication_key = 'updatedAt'
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Collect the results from the sync method
        results = list(self.stream.sync())

        # Verify the results
        expected_results = [
            {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'},
            {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'},
            {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}
        ]
        self.assertEqual(results, expected_results)

        # Verify the bookmark was updated correctly
        mock_update_bookmark.assert_called_with('2023-01-03T00:00:00Z')

    @patch.object(GraphQLStream, 'query')
    def test_get_objects(self, mock_query):
        # Mock data returned by the query
        mock_query.side_effect = [
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'}},
                            {'node': {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'cursor1'
                        }
                    }
                }
            },
            {
                'data': {
                    'testConnection': {
                        'edges': [
                            {'node': {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}}
                        ],
                        'pageInfo': {
                            'hasNextPage': False,
                            'endCursor': None
                        }
                    }
                }
            }
        ]

        # Mock the replication key and initial bookmark
        self.stream.replication_key = 'updatedAt'
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Collect the results from the get_objects method
        results = list(self.stream.get_objects())

        # Verify the results
        expected_results = [
            {'id': '1', 'updatedAt': '2023-01-01T00:00:00Z'},
            {'id': '2', 'updatedAt': '2023-01-02T00:00:00Z'},
            {'id': '3', 'updatedAt': '2023-01-03T00:00:00Z'}
        ]
        self.assertEqual(results, expected_results)

    def test_get_bookmark(self):
        # Mock the bookmark value
        self.stream.get_bookmark = MagicMock(return_value=datetime.datetime(2022, 12, 31))

        # Verify the bookmark value
        self.assertEqual(self.stream.get_bookmark(), datetime.datetime(2022, 12, 31))

    def test_update_bookmark(self):
        # Mock the update_bookmark method
        self.stream.update_bookmark = MagicMock()

        # Call the update_bookmark method
        self.stream.update_bookmark('2023-01-03T00:00:00Z')

        # Verify the update_bookmark method was called with the correct arguments
        self.stream.update_bookmark.assert_called_with('2023-01-03T00:00:00Z')

    @patch('requests.post')
    def test_query_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': {'test': 'value'}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        result = self.stream.query('query { test }')
        self.assertEqual(result, {'test': 'value'})

    @patch('requests.post')
    def test_query_with_errors(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'errors': ['error']}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        with self.assertRaises(ShopifyGraphQLError):
            self.stream.query('query { test }')

    @patch('requests.post')
    def test_query_http_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_post.return_value = mock_response

        with self.assertRaises(requests.exceptions.HTTPError):
            self.stream.query('query { test }')
