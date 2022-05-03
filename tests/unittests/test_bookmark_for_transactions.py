import unittest
from unittest import mock
from singer.utils import strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams import transactions

TRANSACTIONS_OBJECT = Context.stream_objects['transactions']()

class Transaction():
    '''The Transaction object to return.'''
    def __init__(self, id, created_at):
        self.id = id
        self.created_at = created_at

    def to_dict(self):
        return {"id": self.id, "created_at": self.created_at}

tx1 = Transaction("i11", "2021-08-11T01:57:05-04:00")
tx2 = Transaction("i12", "2021-08-12T01:57:05-04:00")
tx3 = Transaction("i21", "2021-08-13T01:57:05-04:00")
tx4 = Transaction("i22", "2021-08-14T01:57:05-04:00")

class TestTransactionsBookmark(unittest.TestCase):

    @mock.patch("tap_shopify.streams.base.Stream.get_bookmark")
    @mock.patch('tap_shopify.streams.transactions.Transactions.get_objects')
    def test_sync(self, mock_get_transactions, mock_get_bookmark):
        '''Verify that the sync returns all the rcords for transactions without filtering through bookmark.'''
        mock_get_transactions.return_value = [tx1, tx2, tx3, tx4]
        mock_get_bookmark.return_value = strptime_to_utc("2021-08-13T01:05:05-04:00")
        expected_sync = [tx1.to_dict(), tx2.to_dict(), tx3.to_dict(), tx4.to_dict()]
        actual_sync = list(TRANSACTIONS_OBJECT.sync())
        self.assertEqual(expected_sync, actual_sync)
