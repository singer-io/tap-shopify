import unittest
from unittest.mock import patch

from singer import utils

from tap_shopify.context import Context
from tap_shopify.streams.transactions import Transactions


def _page(*transactions_per_order):
    return {
        "edges": [{"node": {"transactions": list(txns)}} for txns in transactions_per_order],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }


class TestTransactionsLateArrival(unittest.TestCase):
    def setUp(self):
        Context.config = {"start_date": "2025-01-01T00:00:00Z"}
        Context.state = {"bookmarks": {"transactions": {"createdAt": "2025-03-21T11:12:31.000000Z"}}}
        Context.get_unselected_fields = lambda name: []
        self.stream = Transactions()
        self.stream.date_window_size = 30

    def _run(self, pages, now="2025-03-25T00:00:00Z"):
        with patch.object(Transactions, "call_api", side_effect=pages) as api, \
                patch.object(Transactions, "transform_object", side_effect=lambda o: o), \
                patch("tap_shopify.streams.transactions.utils.now",
                      return_value=utils.strptime_with_tz(now)), \
                patch("singer.write_state"):
            return list(self.stream.get_objects()), api

    def test_transaction_older_than_bookmark_is_emitted(self):
        """A transaction created before the bookmark (payment-first) must not be dropped."""
        old = {"id": "t1", "createdAt": "2021-04-16T19:43:58.000000Z"}
        new = {"id": "t2", "createdAt": "2025-03-22T10:00:00.000000Z"}
        records, _ = self._run([_page([old], [new])])
        self.assertEqual([r["id"] for r in records], ["t1", "t2"])

    def test_bookmark_follows_max_transaction_createdat(self):
        old = {"id": "t1", "createdAt": "2021-04-16T19:43:58.000000Z"}
        new = {"id": "t2", "createdAt": "2025-03-22T10:00:00.000000Z"}
        self._run([_page([old], [new])])
        self.assertEqual(
            Context.state["bookmarks"]["transactions"]["createdAt"],
            "2025-03-22T10:00:00.000000Z",
        )

    def test_bookmark_not_moved_backwards_by_old_transactions(self):
        old = {"id": "t1", "createdAt": "2021-04-16T19:43:58.000000Z"}
        self._run([_page([old])])
        self.assertEqual(
            Context.state["bookmarks"]["transactions"]["createdAt"],
            "2025-03-21T11:12:31.000000Z",
        )

    def test_query_window_starts_one_minute_before_bookmark(self):
        _, api = self._run([_page()])
        query = api.call_args_list[0][0][0]["query"]
        self.assertIn("updated_at:>='2025-03-21 11:11:31", query)

    def test_bookmark_not_moved_backwards_if_clock_behind(self):
        self._run([_page()], now="2025-03-20T00:00:00Z")
        # loop is skipped; bookmark untouched
        self.assertEqual(
            Context.state["bookmarks"]["transactions"]["createdAt"],
            "2025-03-21T11:12:31.000000Z",
        )
