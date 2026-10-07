import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from graphql import parse
from singer import utils
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyAPIError
from tap_shopify.streams.fulfillment_orders import FulfillmentOrders


ORDER_ID = "gid://shopify/FulfillmentOrder/1"
FULFILLMENT_ID = "gid://shopify/Fulfillment/2"
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 1, 2, tzinfo=timezone.utc)


def connection_counts(node, depth=0):
    if not getattr(node, "selection_set", None):
        return 0, depth
    children = node.selection_set.selections
    connection = any(child.name.value == "edges" for child in children)
    counts = [connection_counts(child, depth + int(connection)) for child in children]
    return int(connection) + sum(count for count, _ in counts), max(level for _, level in counts)


class TestFulfillmentOrdersBulk(unittest.TestCase):
    def setUp(self):
        self.original_config = Context.config
        self.original_state = Context.state
        Context.config = {"start_date": utils.strftime(START)}
        Context.state = {}
        self.selection = patch.object(Context, "get_unselected_fields", return_value=[])
        self.selection.start()
        self.write_state = patch("singer.write_state")
        self.write_state.start()
        self.now = patch("tap_shopify.streams.fulfillment_orders.utils.now", return_value=END)
        self.now.start()
        self.addCleanup(self.selection.stop)
        self.addCleanup(self.write_state.stop)
        self.addCleanup(self.now.stop)
        self.stream = FulfillmentOrders()

    def tearDown(self):
        Context.config = self.original_config
        Context.state = self.original_state

    def test_queries_obey_bulk_limits(self):
        queries = self.stream.get_bulk_queries()
        self.assertEqual(len(queries), 3)
        self.assertEqual([connection_counts(parse(query).definitions[0])[0]
                          for query in queries], [3, 4, 3])
        for query in queries:
            self.assertLessEqual(connection_counts(parse(query).definitions[0])[1], 3)
            for unwanted in ("locationsForMove", "pageInfo", "first:", "after:", "$", "nodes {"):
                self.assertNotIn(unwanted, query)
            self.assertIn('query: "%s"', query)
            self.assertIn("includeClosed: true", query)
            self.assertIn("sortKey: UPDATED_AT", query)

    def test_unselected_connections_are_not_queried(self):
        with patch.object(Context, "get_unselected_fields", return_value=[
                "fulfillments", "merchantRequests", "fulfillmentOrdersForMerge", "locationsForMove"]):
            queries = self.stream.get_bulk_queries()
        self.assertEqual(len(queries), 1)
        self.assertEqual(connection_counts(parse(queries[0]).definitions[0])[0], 1)

    def parse_records(self, records, query):
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_lines.return_value = [json.dumps(record).encode("utf-8") for record in records]
        with patch("tap_shopify.streams.fulfillment_orders.requests.get", return_value=response):
            result = list(self.stream.parse_fulfillment_bulk_jsonl(
                "https://example.com/results", query))
        response.raise_for_status.assert_called_once()
        response.__exit__.assert_called_once()
        return result

    def test_reconstructs_nested_jsonl_and_empty_connections(self):
        query = self.stream.get_bulk_queries()[1]
        result = self.parse_records([
            {"id": ORDER_ID},
            {"id": FULFILLMENT_ID, "name": "shipment", "__parentId": ORDER_ID},
            {"id": "gid://shopify/FulfillmentLineItem/3", "quantity": 2, "__parentId": FULFILLMENT_ID},
            {"id": "gid://shopify/BasicEvent/4", "__parentId": FULFILLMENT_ID},
            {"id": "gid://shopify/Fulfillment/5", "__parentId": ORDER_ID},
            {"id": "gid://shopify/FulfillmentOrder/6"},
        ], query)
        self.assertEqual(len(result), 2)
        fulfillment = result[0]["fulfillments"][0]
        self.assertEqual(fulfillment["fulfillmentLineItems"][0]["quantity"], 2)
        self.assertEqual(fulfillment["events"], [{"id": "gid://shopify/BasicEvent/4"}])
        self.assertEqual(result[0]["fulfillments"][1]["events"], [])
        self.assertEqual(result[1]["fulfillments"], [])
        self.assertNotIn("__parentId", json.dumps(result))

    def test_routes_same_type_references_to_correct_connections(self):
        queries = self.stream.get_bulk_queries()
        primary = self.parse_records([
            {"id": ORDER_ID},
            {"id": "gid://shopify/FulfillmentOrder/7", "__parentId": ORDER_ID},
            {"id": "gid://shopify/FulfillmentOrderMerchantRequest/8", "__parentId": ORDER_ID},
        ], queries[0])[0]
        references = self.parse_records([
            {"id": ORDER_ID},
            {"id": FULFILLMENT_ID, "__parentId": ORDER_ID},
            {"id": ORDER_ID, "__parentId": FULFILLMENT_ID},
        ], queries[2])[0]
        self.assertEqual(primary["fulfillmentOrdersForMerge"], [{"id": "gid://shopify/FulfillmentOrder/7"}])
        self.assertEqual(len(primary["merchantRequests"]), 1)
        self.assertEqual(references["fulfillments"][0]["fulfillmentOrders"], [{"id": ORDER_ID}])

    def test_unknown_parent_fails_instead_of_dropping_data(self):
        with self.assertRaises(ShopifyAPIError):
            self.parse_records([{"id": ORDER_ID}, {"id": FULFILLMENT_ID, "__parentId": "unknown"}],
                               self.stream.get_bulk_queries()[1])

    def bulk_results(self):
        return [
            [{"id": ORDER_ID, "updatedAt": "2026-01-01T12:00:00Z"},
             {"id": "gid://shopify/FulfillmentOrder/9", "updatedAt": "2026-01-01T13:00:00Z"}],
            [{"id": "gid://shopify/FulfillmentOrder/9", "fulfillments": []},
             {"id": ORDER_ID, "fulfillments": [{"id": FULFILLMENT_ID, "name": "shipment", "events": []}]}],
            [{"id": ORDER_ID, "fulfillments": [{"id": FULFILLMENT_ID, "fulfillmentOrders": [{"id": ORDER_ID}]}]}],
        ]

    def test_sequential_queries_merge_by_id_and_advance_only_after_emission(self):
        self.stream.submit_and_poll_bulk_query = MagicMock(side_effect=["first", "second", "third"])
        self.stream.parse_fulfillment_bulk_jsonl = MagicMock(side_effect=self.bulk_results())
        self.stream.get_locations_for_move = MagicMock(return_value=[])
        iterator = self.stream.get_objects()
        first = next(iterator)
        self.assertNotIn("updatedAt", Context.state.get("bookmarks", {}).get("fulfillment_orders", {}))
        self.assertEqual(self.stream.submit_and_poll_bulk_query.call_count, 3)
        self.assertEqual(first["fulfillments"][0]["name"], "shipment")
        self.assertEqual(first["fulfillments"][0]["fulfillmentOrders"], [{"id": ORDER_ID}])
        self.assertEqual(len(list(iterator)), 1)
        self.assertEqual(Context.state["bookmarks"]["fulfillment_orders"]["updatedAt"], utils.strftime(END))
        self.stream.get_locations_for_move.assert_any_call(ORDER_ID)

    def window_state(self, queries):
        return {"window_start": utils.strftime(START), "window_end": utils.strftime(END),
                "last_date_window": self.stream.date_window_size,
                "query_hash": hashlib.sha256(json.dumps(queries).encode("utf-8")).hexdigest(),
                "operations": {"0": {"bulk_operation_id": "op1", "status": "COMPLETED"},
                               "1": {"bulk_operation_id": "op2", "status": "RUNNING"}}}

    def test_resumes_all_saved_parts_without_touching_orders_state(self):
        Context.state = {"bookmarks": {
            "orders": {"bulk_operation": {"bulk_operation_id": "orders-op"}},
            "fulfillment_orders": {"bulk_operation": self.window_state(self.stream.get_bulk_queries())}}}
        self.stream.poll_bulk_completion = MagicMock(side_effect=["first", "second"])
        self.stream.submit_and_poll_bulk_query = MagicMock(return_value="third")
        self.stream.parse_fulfillment_bulk_jsonl = MagicMock(side_effect=self.bulk_results())
        with patch.object(Context, "get_unselected_fields", return_value=["locationsForMove"]):
            Context.state["bookmarks"]["fulfillment_orders"]["bulk_operation"]["query_hash"] = hashlib.sha256(
                json.dumps(self.stream.get_bulk_queries()).encode("utf-8")).hexdigest()
            self.assertEqual(len(list(self.stream.get_objects())), 2)
        self.assertEqual(self.stream.poll_bulk_completion.call_count, 2)
        self.stream.submit_and_poll_bulk_query.assert_called_once()
        self.assertEqual(Context.state["bookmarks"]["orders"]["bulk_operation"]["bulk_operation_id"], "orders-op")

    def test_bulk_metadata_does_not_advance_replication_bookmark(self):
        self.stream._bulk_window_state = self.window_state(self.stream.get_bulk_queries())
        self.stream._bulk_query_index = 1
        self.stream.update_bookmark(utils.strftime(END), bulk_op_metadata={
            "bulk_operation_id": "op2", "status": "COMPLETED"})
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertNotIn("updatedAt", bookmark)
        self.assertEqual(bookmark["bulk_operation"]["operations"]["0"]["bulk_operation_id"], "op1")

    def test_submitted_id_is_saved_before_polling(self):
        self.stream._bulk_window_state = self.window_state(self.stream.get_bulk_queries())
        self.stream._bulk_query_index = 2
        response = MagicMock()
        response.json.return_value = {"data": {"bulkOperationRunQuery": {
            "bulkOperation": {"id": "new-op", "status": "CREATED", "createdAt": utils.strftime(START)},
            "userErrors": []}}}
        self.stream.poll_bulk_completion = MagicMock(side_effect=ShopifyAPIError("interrupted"))
        with patch("tap_shopify.streams.orders.requests.post", return_value=response):
            with self.assertRaises(ShopifyAPIError):
                self.stream.submit_and_poll_bulk_query(self.stream.get_bulk_queries()[2], START, END, START)
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertEqual(bookmark["bulk_operation"]["operations"]["2"]["bulk_operation_id"], "new-op")
        self.assertNotIn("updatedAt", bookmark)

    def test_closing_during_emission_keeps_window_uncommitted(self):
        self.stream.submit_and_poll_bulk_query = MagicMock(side_effect=["first", "second", "third"])
        self.stream.parse_fulfillment_bulk_jsonl = MagicMock(side_effect=self.bulk_results())
        self.stream.get_locations_for_move = MagicMock(return_value=[])
        iterator = self.stream.get_objects()
        next(iterator)
        iterator.close()
        self.assertNotIn("updatedAt", Context.state.get("bookmarks", {}).get("fulfillment_orders", {}))

    def test_missing_saved_operation_does_not_commit_the_window(self):
        Context.state = {"bookmarks": {"fulfillment_orders": {
            "bulk_operation": self.window_state(self.stream.get_bulk_queries())}}}
        self.stream.poll_bulk_completion = MagicMock(side_effect=["completed-url", None])
        self.stream.parse_fulfillment_bulk_jsonl = MagicMock(
            return_value=self.bulk_results()[0])
        with self.assertRaises(ShopifyAPIError):
            list(self.stream.get_objects())
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertNotIn("updatedAt", bookmark)
        operations = bookmark["bulk_operation"]["operations"]
        self.assertEqual(operations["0"]["bulk_operation_id"], "op1")
        self.assertNotIn("1", operations)

    def test_failure_leaves_bookmark_unchanged_and_keeps_completed_parts(self):
        queries = self.stream.get_bulk_queries()
        stored = self.window_state(queries)
        Context.state = {"bookmarks": {"fulfillment_orders": {"bulk_operation": copy.deepcopy(stored)}}}
        self.stream.poll_bulk_completion = MagicMock(side_effect=["first", ShopifyAPIError("timeout")])
        self.stream.parse_fulfillment_bulk_jsonl = MagicMock(
            return_value=self.bulk_results()[0])
        with self.assertRaises(ShopifyAPIError):
            list(self.stream.get_objects())
        self.assertEqual(Context.state["bookmarks"]["fulfillment_orders"]["bulk_operation"], stored)
        self.assertNotIn("updatedAt", Context.state["bookmarks"]["fulfillment_orders"])

    def test_empty_completed_operation_is_not_resubmitted(self):
        with patch.object(Context, "get_unselected_fields", return_value=["fulfillments", "locationsForMove"]):
            def complete(*_args):
                self.stream.update_bookmark(utils.strftime(START), bulk_op_metadata={
                    "bulk_operation_id": "empty", "status": "COMPLETED"})
                return None
            self.stream.submit_and_poll_bulk_query = MagicMock(side_effect=complete)
            self.assertEqual(list(self.stream.get_objects()), [])
        self.stream.submit_and_poll_bulk_query.assert_called_once()

    def test_locations_paginates_outer_and_both_line_item_connections(self):
        def connection(nodes, cursor=None):
            return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}
        location = {"location": {"id": "location1"},
                    "availableLineItems": connection([{"id": "a1"}], "a-cursor"),
                    "unavailableLineItems": connection([{"id": "u1"}], "u-cursor")}
        self.stream.call_api = MagicMock(side_effect=[
            {"locationsForMove": {"edges": [{"node": location}],
                                  "pageInfo": {"hasNextPage": True, "endCursor": "location-cursor"}}},
            {"locationsForMove": {"edges": [{"node": {"availableLineItems": connection([{"id": "a2"}])}}]}},
            {"locationsForMove": {"edges": [{"node": {"unavailableLineItems": connection([{"id": "u2"}])}}]}},
            {"locationsForMove": {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}},
        ])
        result = self.stream.get_locations_for_move(ORDER_ID)
        self.assertEqual(result[0]["availableLineItems"], [{"id": "a1"}, {"id": "a2"}])
        self.assertEqual(result[0]["unavailableLineItems"], [{"id": "u1"}, {"id": "u2"}])
        calls = self.stream.call_api.call_args_list
        self.assertEqual(calls[1].args[0]["after"], "a-cursor")
        self.assertEqual(calls[2].args[0]["after"], "u-cursor")
        self.assertEqual(calls[3].args[0]["after"], "location-cursor")
        for call in calls:
            parse(call.kwargs["query"])

    def check_sync_order(self, currently_syncing=None):
        import tap_shopify
        names = ["fulfillment_orders", "products", "orders"]
        catalog = {"streams": [{"tap_stream_id": name} for name in names]}
        if currently_syncing:
            Context.state = {"bookmarks": {"currently_sync_stream": currently_syncing}}
        with patch.object(Context, "catalog", catalog), \
                patch.object(Context, "stream_objects", {name: MagicMock() for name in names}), \
                patch.object(Context, "is_selected", return_value=False), \
                patch.object(Context, "counts", {}), \
                patch("tap_shopify.SDC_KEYS", []), \
                patch("tap_shopify.initialize_shopify_client", return_value={}):
            tap_shopify.sync()
        return [entry["tap_stream_id"] for entry in catalog["streams"]]

    def test_fresh_sync_runs_orders_before_fulfillment_orders(self):
        self.assertEqual(self.check_sync_order(), ["products", "orders", "fulfillment_orders"])

    def test_interrupted_fulfillment_stream_still_resumes_first(self):
        self.assertEqual(self.check_sync_order("fulfillment_orders"),
                         ["fulfillment_orders", "products", "orders"])