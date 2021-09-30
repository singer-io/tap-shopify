from unittest import mock

import pyactiveresource
import tap_shopify
import unittest

# Mock args
class Args():
    def __init__(self):
        self.discover = True
        self.catalog = False
        self.config = {'api_key': 'test', 'shop': 'shop'}
        self.state = False

def resource_not_found_raiser():
    raise pyactiveresource.connection.ResourceNotFound

def unauthorized_access_raiser():
    raise pyactiveresource.connection.UnauthorizedAccess

def connection_error_raiser():
    raise pyactiveresource.connection.ConnectionError

@mock.patch('tap_shopify.utils.parse_args')
@mock.patch('tap_shopify.discover', side_effect=tap_shopify.discover)
@mock.patch("builtins.print")
class TestTokenInDiscoverMode(unittest.TestCase):

    @mock.patch('tap_shopify.initialize_shopify_client', side_effect=resource_not_found_raiser)
    def test_resource_not_found(self, mocked_client, mocked_print, mocked_discover, mocked_args):
        '''
            Verify exception is raised for ResourceNotFound with proper error message and
            test that discover mode is called 
        '''
        mocked_args.return_value = Args()
        try:
            tap_shopify.main()
        except tap_shopify.ShopifyError as e:
            self.assertEqual(str(e), 'ResourceNotFound\nEnsure shop is entered correctly')
            self.assertEqual(mocked_discover.call_count, 1)
            self.assertEqual(mocked_client.call_count, 1)
            self.assertEqual(mocked_print.call_count, 0)

    @mock.patch('tap_shopify.initialize_shopify_client', side_effect=unauthorized_access_raiser)
    def test_unauthorized_access(self, mocked_client, mocked_print, mocked_discover, mocked_args):
        '''
            Verify exception is raised for UnauthorizedAccess with proper error message and
            test that discover mode is called 
        '''
        mocked_args.return_value = Args()
        try:
            tap_shopify.main()
        except tap_shopify.ShopifyError as e:
            self.assertEqual(str(e), 'UnauthorizedAccess\nInvalid access token - Re-authorize the connection')
            self.assertEqual(mocked_discover.call_count, 1)
            self.assertEqual(mocked_client.call_count, 1)
            self.assertEqual(mocked_print.call_count, 0)

    @mock.patch('tap_shopify.initialize_shopify_client', side_effect=connection_error_raiser)
    def test_connection_error(self, mocked_client, mocked_print, mocked_discover, mocked_args):
        '''
            Verify exception is raised for ConnectionError with proper error message and
            test that discover mode is called 
        '''
        mocked_args.return_value = Args()
        try:
            tap_shopify.main()
        except tap_shopify.ShopifyError as e:
            self.assertEqual(str(e), 'ConnectionError\n')
            self.assertEqual(mocked_discover.call_count, 1)
            self.assertEqual(mocked_client.call_count, 1)
            self.assertEqual(mocked_print.call_count, 0)

    @mock.patch('tap_shopify.initialize_shopify_client')
    def test_no_error(self, mocked_client, mocked_print, mocked_discover, mocked_args):
        '''
            Verify that if no error during discover then print should be called once for
            writing catalog
        '''
        mocked_args.return_value = Args()
        tap_shopify.main()
        self.assertEqual(mocked_discover.call_count, 1)
        self.assertEqual(mocked_client.call_count, 1)
        self.assertEqual(mocked_print.call_count, 1)
