import unittest
from tap_shopify.streams.transactions import canonicalize

class TestTransactionCanonicalize(unittest.TestCase):
    def test_unmodified_if_not_present(self):
        # Note: Canonicalize has a side effect with pop(), must copy test
        # record to compare
        record = {"receipt": {"foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize(record, "token")
        self.assertEqual(record, expected_record)

    def test_unmodified_if_only_lower_exists(self):
        record = {"receipt": {"foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize(record, "foo")
        self.assertEqual(record, expected_record)

    def test_lowercases_if_capital_only_exists(self):
        record = {"receipt": {"Foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize(record, "foo")
        self.assertEqual(record, expected_record)

    def test_null_receipt_record(self):
        record = {"receipt": None}
        expected_record = {"receipt": None}
        canonicalize(record, "foo")
        self.assertEqual(record, expected_record)

    def test_removes_uppercase_if_both_exist_and_are_equal(self):
        record = {"receipt": {"Foo": "bar", "foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize(record, "foo")
        self.assertEqual(record, expected_record)

    def test_throws_if_both_exist_and_are_not_equal(self):
        record = {"receipt": {"Foo": "bark", "foo": "bar"}, "id": 2}
        with self.assertRaises(ValueError):
            canonicalize(record, "foo")
