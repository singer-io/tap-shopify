import unittest
from tap_shopify.streams.transactions import canonicalize_receipts

class TestTransactionCanonicalizeReceipts(unittest.TestCase):
    def test_unmodified_if_not_present(self):
        # Note: Canonicalize_Receipts has a side effect with pop(), must copy test
        # record to compare
        record = {"receipt": {"foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize_receipts(record)
        self.assertEqual(record, expected_record)

    def test_unmodified_if_only_lower_exists(self):
        record = {"receipt": {"foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize_receipts(record)
        self.assertEqual(record, expected_record)

    def test_lowercases_if_capital_only_exists(self):
        record = {"receipt": {"Foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize_receipts(record)
        self.assertEqual(record, expected_record)

    def test_removes_uppercase_if_both_exist_and_are_equal(self):
        record = {"receipt": {"Foo": "bar", "foo": "bar"}, "id": 2}
        expected_record = {"receipt": {"foo": "bar"}, "id": 2}
        canonicalize_receipts(record)
        self.assertEqual(record, expected_record)

    def test_throws_if_both_exist_and_are_not_equal(self):
        record = {"receipt": {"Foo": "bark", "foo": "bar"}, "id": 2}
        with self.assertRaises(ValueError):
            canonicalize_receipts(record)
