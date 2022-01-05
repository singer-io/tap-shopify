import socket
import tap_shopify
from unittest import mock
import pyactiveresource
import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import get_request_timeout
from tap_shopify.streams.inventory_items import InventoryItems
from tap_shopify.streams.inventory_levels import InventoryLevels
from tap_shopify.streams.locations import Locations
from tap_shopify.streams.order_refunds import OrderRefunds
from tap_shopify.streams.transactions import Transactions
from tap_shopify.streams.abandoned_checkouts import AbandonedCheckouts
from tap_shopify.streams.metafields import Metafields
from tap_shopify.streams.metafields import get_metafields
import unittest

class TestTimeoutValue(unittest.TestCase):
    """
        Verify the timeout value is set as expected by the tap
    """

    def test_timeout_value_not_passed_in_config(self):
        """
            Test case to verify that the default value is used when we do not pass request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 300)

    def test_timeout_int_value_passed_in_config(self):
        """
            Test case to verify that the value we passed on config is set as request timeout value
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": 100
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 100)

    def test_timeout_string_value_passed_in_config(self):
        """
            Test case to verify that the value we passed on config is set as request timeout value
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": "100"
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 100)

    def test_timeout_empty_value_passed_in_config(self):
        """
            Test case to verify that the default value is used when pass empty request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": ""
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 300)

    def test_timeout_0_value_passed_in_config(self):
        """
            Test case to verify that the default value is used when pass 0 as request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": 0.0
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 300)

    def test_timeout_string_0_value_passed_in_config(self):
        """
            Test case to verify that the default value is used when pass string 0 as request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": "0.0"
        }

        # initialize base class
        timeout = get_request_timeout()
        # verify the timeout is set as expected
        self.assertEquals(timeout, 300)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_value_not_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the default value is used when we do not pass request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(300)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_int_value_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the value we passed on config is set as request timeout value
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": 100
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(100)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_string_value_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the value we passed on config is set as request timeout value
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": "100"
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(100)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_empty_value_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the default value is used when pass empty request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": ""
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(300)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_0_value_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the default value is used when pass 0 as request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": 0.0
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(300)

    @mock.patch("shopify.Shop.set_timeout")
    @mock.patch("shopify.Shop.current")
    def test_timeout_string_0_value_passed_in_config__initialize_shopify_client(self, mocked_current, mocked_set_timeout):
        """
            Test case to verify that the default value is used when pass string 0 as request timeout value from config
        """
        # initialize config
        Context.config = {
            "start_date": "2021-01-01",
            "api_key": "test_api_key",
            "shop": "test_shop",
            "results_per_page": 50,
            "request_timeout": "0.0"
        }

        # function call
        tap_shopify.initialize_shopify_client()
        # verify the timeout is set as expected
        mocked_set_timeout.assert_called_with(300)

class TestTimeoutBackoff(unittest.TestCase):
    """
        Verify the tap backoff for 5 times when timeout error occurs
    """

    @mock.patch("time.sleep")
    @mock.patch("shopify.Checkout.find")
    def test_AbandonedCheckouts_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize 'AbandonedCheckouts' as it calls the function 'call_api' from the base class
        abandoned_checkouts = AbandonedCheckouts()
        try:
            # function call
            abandoned_checkouts.call_api({})
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.InventoryItem.find")
    def test_InventoryItems_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize class
        inventory_items = InventoryItems()
        try:
            # function call
            inventory_items.get_inventory_items([1, 2, 3])
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_InventoryLevels_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize class
        inventory_levels = InventoryLevels()
        try:
            # function call
            inventory_levels.api_call_for_inventory_levels(1, 'test')
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_Locations_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize class
        locations = Locations()
        try:
            # function call
            locations.replication_object.find()
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Order.metafields")
    def test_Metafields_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        try:
            # function call
            get_metafields(shopify.Order, 1, shopify.Order, 100)
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Refund.find")
    def test_OrderRefunds_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize class
        order_refunds = OrderRefunds()
        try:
            # function call
            order_refunds.get_refunds(shopify.Product, 1)
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_Transactions_pyactiveresource_error_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        # initialize class
        locations = Transactions()
        try:
            # function call
            locations.replication_object.find()
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Shop.current")
    def test_Shop_pyactiveresource_error_timeout_backoff(self, mocked_current, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'pyactiveresource.connection.Error' error occurs
        """
        # mock 'Shop' call and raise timeout error
        mocked_current.side_effect = pyactiveresource.connection.Error('urlopen error _ssl.c:1074: The handshake operation timed out')

        Context.config = {
            "api_key": "test_api_key",
            "shop": "test_shop"
        }
        try:
            # function call
            tap_shopify.initialize_shopify_client()
        except pyactiveresource.connection.Error:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_current.call_count, 5)

    """
        Verify the tap backoff for 5 times when timeout error occurs
    """

    @mock.patch("time.sleep")
    @mock.patch("shopify.Checkout.find")
    def test_AbandonedCheckouts_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize 'AbandonedCheckouts' as it calls the function 'call_api' from the base class
        abandoned_checkouts = AbandonedCheckouts()
        try:
            # function call
            abandoned_checkouts.call_api({})
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.InventoryItem.find")
    def test_InventoryItems_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize class
        inventory_items = InventoryItems()
        try:
            # function call
            inventory_items.get_inventory_items([1, 2, 3])
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_InventoryLevels_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize class
        inventory_levels = InventoryLevels()
        try:
            # function call
            inventory_levels.api_call_for_inventory_levels(1, 'test')
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_Locations_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize class
        locations = Locations()
        try:
            # function call
            locations.replication_object.find()
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Order.metafields")
    def test_Metafields_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        try:
            # function call
            get_metafields(shopify.Order, 1, shopify.Order, 100)
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Refund.find")
    def test_OrderRefunds_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize class
        order_refunds = OrderRefunds()
        try:
            # function call
            order_refunds.get_refunds(shopify.Product, 1)
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("pyactiveresource.activeresource.ActiveResource.find")
    def test_Transactions_socket_timeout_backoff(self, mocked_find, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'find' and raise timeout error
        mocked_find.side_effect = socket.timeout("The read operation timed out")

        # initialize class
        locations = Transactions()
        try:
            # function call
            locations.replication_object.find()
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_find.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("shopify.Shop.current")
    def test_Shop_socket_timeout_backoff(self, mocked_current, mocked_sleep):
        """
            Test case to verify that we backoff for 5 times when 'socket.timeout' error occurs
        """
        # mock 'Shop' call and raise timeout error
        mocked_current.side_effect = socket.timeout("The read operation timed out")

        Context.config = {
            "api_key": "test_api_key",
            "shop": "test_shop"
        }
        try:
            # function call
            tap_shopify.initialize_shopify_client()
        except socket.timeout:
            pass

        # verify we backoff 5 times
        self.assertEquals(mocked_current.call_count, 5)
