from copy import deepcopy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import MagicMock, patch

from graphql import parse
import requests

from tap_shopify import _is_fulfillment_scope_error, order_bulk_streams, shuffle_streams
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyAPIError
from tap_shopify.streams.fulfillment_orders import FulfillmentOrders


class TestFulfillmentOrdersBulk(unittest.TestCase):
    def setUp(self):
        self.config_patch = patch.object(Context, "config", {
            "start_date": "2026-01-01T00:00:00Z",
        })
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.state_patch = patch.object(Context, "state", {})
        self.state_patch.start()
        self.addCleanup(self.state_patch.stop)
        self.selection_patch = patch.object(Context, "get_unselected_fields", return_value=[])
        self.selection_patch.start()
        self.addCleanup(self.selection_patch.stop)
        self.write_patch = patch("singer.write_state")
        self.write_patch.start()
        self.addCleanup(self.write_patch.stop)
        self.stream = FulfillmentOrders()

    def response(self, records):
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_lines.return_value = [json.dumps(record).encode() for record in records]
        return response

    def root(self, record_id="root", **fields):
        return {
            "id": record_id, "updatedAt": "2026-01-01T01:00:00Z",
            "bulk_fulfillmentOrders": "FulfillmentOrder", **fields,
        }

    def child(self, record_id, parent_id, connection, **fields):
        return {
            "id": record_id, "__parentId": parent_id,
            f"bulk_{connection}": "Node", **fields,
        }

    @patch.object(Context, "get_unselected_fields", return_value=[])
    def test_bulk_queries_respect_connection_limits(self, _selected):
        queries = FulfillmentOrders().get_bulk_queries()
        self.assertEqual(len(queries), 2)
        for query in queries:
            connections = []

            def check_selection(selection, depth=0):
                for field in selection.selections:
                    if field.selection_set:
                        is_connection = any(
                            child.name.value == "edges"
                            for child in field.selection_set.selections
                        )
                        child_depth = depth + int(is_connection)
                        if is_connection:
                            connections.append(field.name.value)
                            self.assertLessEqual(child_depth, 3)
                        check_selection(field.selection_set, child_depth)

            check_selection(parse(query).definitions[0].selection_set)
            self.assertLessEqual(len(connections), 5)
            self.assertNotIn("$", query)
            self.assertNotIn("pageInfo", query)
            self.assertNotIn("first:", query)
            self.assertNotIn("locationsForMove", query)

    def test_unselected_connections_are_not_exported(self):
        Context.get_unselected_fields.return_value = [
            "fulfillments", "merchantRequests", "fulfillmentOrdersForMerge",
        ]
        queries = self.stream.get_bulk_queries()
        self.assertEqual(len(queries), 1)
        self.assertNotIn("bulk_fulfillments", queries[0])
        self.assertNotIn("merchantRequests", queries[0])

    @patch("tap_shopify.streams.fulfillment_orders.requests.get")
    def test_parse_nested_jsonl_and_duplicate_reference_ids(self, mock_get):
        mock_get.return_value = self.response([
            self.root(),
            self.child("fulfillment1", "root", "fulfillments"),
            self.child("root", "fulfillment1", "fulfillmentOrders"),
            self.child("line1", "fulfillment1", "fulfillmentLineItems", quantity=2),
            self.child("event1", "fulfillment1", "events"),
            self.child("fulfillment2", "root", "fulfillments"),
            self.child("line2", "fulfillment2", "fulfillmentLineItems"),
            self.root("other"),
        ])
        records = list(self.stream.parse_bulk_jsonl("url", self.stream.get_bulk_queries()[1]))
        self.assertEqual(len(records), 2)
        first, second = records[0]["fulfillments"]
        self.assertEqual(first["fulfillmentOrders"], [{"id": "root"}])
        self.assertEqual(first["fulfillmentLineItems"], [{"id": "line1", "quantity": 2}])
        self.assertEqual(first["events"], [{"id": "event1"}])
        self.assertEqual(second["fulfillmentLineItems"], [{"id": "line2"}])
        self.assertEqual(second["events"], [])
        self.assertEqual(records[1]["fulfillments"], [])
        self.assertNotIn("bulk_", json.dumps(records))
        self.assertNotIn("__parentId", json.dumps(records))

    @patch("tap_shopify.streams.fulfillment_orders.requests.get")
    def test_parse_merchant_requests_and_merge_orders(self, mock_get):
        mock_get.return_value = self.response([
            self.root(status="OPEN"),
            self.child("merchant", "root", "merchantRequests"),
            self.child("root", "root", "fulfillmentOrdersForMerge"),
        ])
        records = list(self.stream.parse_bulk_jsonl("url", self.stream.get_bulk_queries()[0]))
        self.assertEqual(records[0]["merchantRequests"], [{"id": "merchant"}])
        self.assertEqual(records[0]["fulfillmentOrdersForMerge"], [{"id": "root"}])

    @patch("tap_shopify.streams.fulfillment_orders.requests.get")
    def test_unknown_parent_rejected(self, mock_get):
        mock_get.return_value = self.response([
            self.root(), self.child("line", "missing", "fulfillmentLineItems"),
        ])
        with self.assertRaises(ShopifyAPIError):
            list(self.stream.parse_bulk_jsonl("url", self.stream.get_bulk_queries()[1]))

    @patch("tap_shopify.streams.fulfillment_orders.requests.get")
    def test_expired_download_clears_operation_state(self, mock_get):
        Context.state = {"bookmarks": {"fulfillment_orders": {"bulk_operation": {}}}}
        response = self.response([])
        response.status_code = 403
        response.raise_for_status.side_effect = requests.HTTPError("expired")
        mock_get.return_value = response
        with self.assertRaises(requests.HTTPError):
            list(self.stream.parse_bulk_jsonl("url", self.stream.get_bulk_queries()[0]))
        self.assertNotIn("bulk_operation", Context.state["bookmarks"]["fulfillment_orders"])

    def test_poll_running_then_completed(self):
        self.stream.bulk_request = MagicMock(side_effect=[
            {"bulkOperation": {"status": "RUNNING"}},
            {"bulkOperation": {"status": "COMPLETED", "url": "url", "objectCount": "1"}},
        ])
        with patch("tap_shopify.streams.fulfillment_orders.time.sleep") as sleep:
            self.assertEqual(self.stream.poll_bulk_completion("op"), "url")
        sleep.assert_called_once_with(60)
        for call in self.stream.bulk_request.call_args_list:
            self.assertEqual(call.args[1], {"id": "op"})

    def test_poll_failed_preserves_window_and_marks_operation(self):
        Context.state = {"bookmarks": {"fulfillment_orders": {
            "bulk_operation": {"operations": {"0": {"id": "op", "status": "RUNNING"}}},
        }}}
        self.stream.bulk_request = MagicMock(return_value={
            "bulkOperation": {"status": "FAILED", "errorCode": "ACCESS_DENIED"},
        })
        with self.assertRaisesRegex(ShopifyAPIError, "ACCESS_DENIED") as error:
            self.stream.poll_bulk_completion("op")
        self.assertTrue(_is_fulfillment_scope_error(error.exception))
        self.assertEqual(Context.state["bookmarks"]["fulfillment_orders"][
            "bulk_operation"]["operations"]["0"]["status"], "FAILED")

    def test_poll_empty_result_and_timeout(self):
        self.stream.bulk_request = MagicMock(return_value={
            "bulkOperation": {"status": "COMPLETED", "url": None, "objectCount": "0"},
        })
        self.assertIsNone(self.stream.poll_bulk_completion("op"))
        with self.assertRaisesRegex(ShopifyAPIError, "op timed out"):
            self.stream.poll_bulk_completion("op", timeout=0)

    def test_missing_operation_clears_state(self):
        Context.state = {"bookmarks": {"fulfillment_orders": {"bulk_operation": {}}}}
        self.stream.bulk_request = MagicMock(return_value={"bulkOperation": None})
        with self.assertRaisesRegex(ShopifyAPIError, "not found"):
            self.stream.poll_bulk_completion("op")
        self.assertNotIn("bulk_operation", Context.state["bookmarks"]["fulfillment_orders"])

    @patch("tap_shopify.streams.fulfillment_orders.requests.post")
    def test_request_uses_2026_10_and_refreshes_token(self, mock_post):
        Context.config.update({"shop": "test", "access_token": "old"})
        unauthorized = self.response([])
        unauthorized.status_code = 401
        completed = self.response([])
        completed.status_code = 200
        completed.json.return_value = {"data": {"bulkOperation": {"id": "op"}}}
        mock_post.side_effect = [unauthorized, completed]
        with patch.object(Context, "client", MagicMock(access_token="new")) as client:
            self.stream.bulk_request("query", {"id": "op"})
            client.refresh_token.assert_called_once()
            client.reinitialize_session.assert_called_once()
        self.assertEqual(Context.config["access_token"], "new")
        self.assertIn("/2026-10/", mock_post.call_args.args[0])
        self.assertEqual(mock_post.call_args.kwargs["headers"]["X-Shopify-Access-Token"], "new")

    @patch("tap_shopify.streams.fulfillment_orders.requests.post")
    def test_request_rejects_graphql_errors(self, mock_post):
        Context.config["shop"] = "test"
        response = self.response([])
        response.status_code = 200
        response.json.return_value = {"errors": [{"message": "Access denied"}]}
        mock_post.return_value = response
        with self.assertRaisesRegex(ShopifyAPIError, "Access denied"):
            self.stream.bulk_request("query")

    def test_submit_rejects_user_errors(self):
        self.stream.bulk_request = MagicMock(return_value={
            "bulkOperationRunQuery": {"userErrors": [{"message": "Access denied"}]},
        })
        with self.assertRaisesRegex(ShopifyAPIError, "Access denied"):
            self.stream.submit_bulk_query("query")

    def test_submit_requests_grouped_bulk_output(self):
        self.stream.bulk_request = MagicMock(return_value={
            "bulkOperationRunQuery": {"bulkOperation": {"id": "op"}},
        })
        self.stream.submit_bulk_query("query")
        mutation = parse(self.stream.bulk_request.call_args.args[0])
        operation = mutation.definitions[0]
        bulk_operation = operation.selection_set.selections[0]
        arguments = {argument.name.value: argument.value for argument in bulk_operation.arguments}
        self.assertEqual(arguments["groupObjects"].value, True)

    def test_fulfillment_scope_errors_detect_direct_and_chained_messages(self):
        direct_error = ShopifyAPIError("Fulfillment GraphQL error: Access denied")
        wrapped_error = ShopifyAPIError("An error occurred with the GraphQL API.")
        wrapped_error.__cause__ = ShopifyAPIError("Access denied for fulfillmentOrders")
        unrelated_error = ShopifyAPIError("Request failed")
        self.assertTrue(_is_fulfillment_scope_error(direct_error))
        self.assertTrue(_is_fulfillment_scope_error(wrapped_error))
        self.assertFalse(_is_fulfillment_scope_error(unrelated_error))

    def mock_sync(self, empty=False):
        self.stream.submit_bulk_query = MagicMock(side_effect=[
            {"id": "op0", "status": "CREATED"}, {"id": "op1", "status": "CREATED"},
        ])
        self.stream.poll_bulk_completion = MagicMock(return_value=None if empty else "url")
        self.stream.parse_bulk_jsonl = MagicMock(side_effect=[
            iter([{"id": "root", "updatedAt": "2026-01-01T01:00:00Z", "status": "OPEN"}]),
            iter([{"id": "root", "updatedAt": "2026-01-01T01:00:00Z", "fulfillments": []}]),
        ])
        self.stream.get_locations_for_move = MagicMock(return_value=[])
        now_patch = patch("tap_shopify.streams.fulfillment_orders.utils.now", return_value=(
            datetime(2026, 1, 2, tzinfo=timezone.utc)
        ))
        now_patch.start()
        self.addCleanup(now_patch.stop)

    def test_sync_merges_before_emitting_and_advancing_bookmark(self):
        self.mock_sync()
        objects = self.stream.get_objects()
        record = next(objects)
        self.assertEqual(record["status"], "OPEN")
        self.assertEqual(record["fulfillments"], [])
        self.assertEqual(record["locationsForMove"], [])
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertNotIn("updatedAt", bookmark)
        self.assertEqual(len(bookmark["bulk_operation"]["operations"]), 2)
        self.assertEqual(list(objects), [])
        self.assertEqual(bookmark["updatedAt"], "2026-01-02T00:00:00.000000Z")
        self.assertNotIn("bulk_operation", bookmark)

    def test_resume_uses_saved_ids_and_exact_window_end(self):
        self.mock_sync()
        objects = self.stream.get_objects()
        next(objects)
        state = deepcopy(Context.state)
        objects.close()
        Context.state = state
        self.mock_sync()
        with patch("tap_shopify.streams.fulfillment_orders.utils.now", return_value=(
            datetime(2026, 1, 2, 1, tzinfo=timezone.utc)
        )):
            resumed = self.stream.get_objects()
            next(resumed)
            self.stream.submit_bulk_query.assert_not_called()
            self.assertEqual(self.stream.poll_bulk_completion.call_args_list[0].args, ("op0",))
            self.assertEqual(Context.state["bookmarks"]["fulfillment_orders"][
                "bulk_operation"]["window_end"], "2026-01-02T00:00:00.000000Z")
            resumed.close()

    def test_poll_interruption_saves_id_without_bookmark_advance(self):
        self.mock_sync()
        self.stream.poll_bulk_completion.side_effect = ShopifyAPIError("timeout")
        with self.assertRaises(ShopifyAPIError):
            list(self.stream.get_objects())
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertNotIn("updatedAt", bookmark)
        self.assertEqual(bookmark["bulk_operation"]["operations"]["0"]["id"], "op0")

    def test_empty_window_advances(self):
        self.mock_sync(empty=True)
        self.assertEqual(list(self.stream.get_objects()), [])
        self.assertEqual(Context.state["bookmarks"]["fulfillment_orders"][
            "updatedAt"], "2026-01-02T00:00:00.000000Z")

    def test_unselected_location_field_skips_fallback(self):
        self.mock_sync()
        Context.get_unselected_fields.return_value = ["locationsForMove"]
        record = list(self.stream.get_objects())[0]
        self.assertNotIn("locationsForMove", record)
        self.stream.get_locations_for_move.assert_not_called()

    def test_location_failure_does_not_advance_bookmark(self):
        self.mock_sync()
        self.stream.get_locations_for_move.side_effect = ShopifyAPIError("location failed")
        with self.assertRaisesRegex(ShopifyAPIError, "location failed"):
            list(self.stream.get_objects())
        bookmark = Context.state["bookmarks"]["fulfillment_orders"]
        self.assertNotIn("updatedAt", bookmark)
        self.assertIn("bulk_operation", bookmark)

    def test_changed_selection_discards_saved_operations(self):
        self.mock_sync()
        objects = self.stream.get_objects()
        next(objects)
        objects.close()
        self.mock_sync()
        Context.get_unselected_fields.return_value = ["fulfillments"]
        self.assertEqual(len(list(self.stream.get_objects())), 1)
        self.stream.submit_bulk_query.assert_called_once()

    def test_saved_failed_operation_is_resubmitted(self):
        self.mock_sync()
        objects = self.stream.get_objects()
        next(objects)
        objects.close()
        Context.state["bookmarks"]["fulfillment_orders"][
            "bulk_operation"]["operations"]["1"]["status"] = "FAILED"
        self.mock_sync()
        self.assertEqual(len(list(self.stream.get_objects())), 1)
        self.stream.submit_bulk_query.assert_called_once()

    def test_mismatched_roots_do_not_emit_or_advance(self):
        self.mock_sync()
        self.stream.parse_bulk_jsonl.side_effect = [
            iter([{"id": "root"}]), iter([]),
        ]
        with self.assertRaisesRegex(ShopifyAPIError, "roots changed"):
            list(self.stream.get_objects())
        self.assertNotIn("updatedAt", Context.state["bookmarks"]["fulfillment_orders"])

    def test_location_and_line_item_pagination(self):
        def connection(nodes, cursor=None):
            return {"nodes": nodes, "pageInfo": {
                "hasNextPage": cursor is not None, "endCursor": cursor,
            }}

        requests_seen = []

        def fetch(query, variables):
            parse(query)
            requests_seen.append((query, dict(variables)))
            if "fulfillmentMoveLocations" in query:
                location_id = "loc2" if variables.get("after") else "loc1"
                result = connection([{"location": {"id": location_id}, "movable": True}],
                                    None if variables.get("after") else "location_cursor")
            else:
                field = (
                    "unavailableLineItems" if "unavailableLineItems(" in query
                    else "availableLineItems"
                )
                result = {"nodes": [{field: connection(
                    [{"id": "line2" if variables.get("after") else "line1"}],
                    None if variables.get("after") else "line_cursor",
                )}]}
            return {"fulfillmentOrder": {"locationsForMove": result}}

        self.stream.bulk_request = MagicMock(side_effect=fetch)
        locations = self.stream.get_locations_for_move("root")
        self.assertEqual(len(locations), 2)
        for location in locations:
            for field in ("availableLineItems", "unavailableLineItems"):
                self.assertEqual(location[field], [{"id": "line1"}, {"id": "line2"}])
        self.assertEqual(len(requests_seen), 10)
        self.assertEqual(requests_seen[-1][1]["locationIds"], ["loc2"])

    def test_orders_precedes_fulfillment_with_resume_priority_preserved(self):
        with patch.object(Context, "catalog", {"streams": [
            {"tap_stream_id": name}
            for name in ["customers", "fulfillment_orders", "locations", "orders", "products"]
        ]}):
            order_bulk_streams()
            names = [stream["tap_stream_id"] for stream in Context.catalog["streams"]]
            self.assertEqual(names, [
                "customers", "locations", "orders", "fulfillment_orders", "products",
            ])
            order_bulk_streams()
            self.assertEqual(names, [
                stream["tap_stream_id"] for stream in Context.catalog["streams"]
            ])
            shuffle_streams("fulfillment_orders")
            self.assertEqual(Context.catalog["streams"][0]["tap_stream_id"], "fulfillment_orders")


if __name__ == "__main__":
    unittest.main()