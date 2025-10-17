import unittest
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class TestShopifyDateWindowHandling(unittest.TestCase):

    def test_no_date_window_value(self):
        """Test that no value for date_window is handled correctly."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 30)

    def test_valid_int_date_window_value(self):
        """Test that valid integer value for date_window is handled correctly."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": 10
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 10)

    def test_valid_str_date_window_value(self):
        """Test that valid str value for date_window is handled correctly.."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": "10"
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 10)

    def test_valid_float_date_window_value(self):
        """Test that valid float value for date_window is handled correctly."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": "11.00"
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 11)

    def test_zero_str_date_window_value(self):
        """Test that valid zero string value for date_window is handled correctly."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": "0"
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 30)

    def test_zero_int_date_window_value(self):
        """Test that valid zero integer value for date_window is handled correctly."""
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": 0
        }
        streams = Stream()
        self.assertEqual(streams.date_window_size, 30)
