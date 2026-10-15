import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from graphql import InlineFragmentNode, parse

from tap_shopify.context import Context
from tap_shopify.streams.order_refunds import (MAX_NODES_PER_QUERY,
                                               REFUND_SHIPPING_LINES_PAGE_QUERY,
                                               REFUND_SHIPPING_LINES_QUERY,
                                               OrderRefunds)

SCHEMA_PATH = Path("tap_shopify/schemas/order_refunds.json")


def money_bag(amount):
    money = {"amount": amount, "currencyCode": "USD"}
    return {"presentmentMoney": dict(money), "shopMoney": dict(money)}


def refund(refund_id, updated_at="2026-01-01T12:00:00Z"):
    return {"id": f"gid://shopify/Refund/{refund_id}", "updatedAt": updated_at}


def shipping_line_node(line_id):
    return {
        "id": f"gid://shopify/RefundShippingLine/{line_id}",
        "subtotalAmountSet": money_bag("5.0"),
        "taxAmountSet": money_bag("0.5"),
        "shippingLine": {"id": "gid://shopify/ShippingLine/7", "title": "Standard"},
    }


def orders_page(*refunds):
    return {
        "edges": [{"node": {"refunds": list(refunds)}}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }


def nodes_response(shipping_lines_by_refund, page_info_by_refund=None):
    page_info_by_refund = page_info_by_refund or {}
    return [
        {
            "id": refund_id,
            "refundShippingLines": {
                "edges": [{"node": node} for node in nodes],
                "pageInfo": page_info_by_refund.get(
                    refund_id, {"hasNextPage": False, "endCursor": None}
                ),
            },
        }
        for refund_id, nodes in shipping_lines_by_refund.items()
    ]


def field_tree(selection_set):
    """Field names of a GraphQL selection set as a nested dict, through inline fragments."""
    tree = {}
    for selection in selection_set.selections:
        if isinstance(selection, InlineFragmentNode):
            tree.update(field_tree(selection.selection_set))
        elif selection.selection_set:
            tree[selection.name.value] = field_tree(selection.selection_set)
        else:
            tree[selection.name.value] = None
    return tree


def schema_tree(schema):
    """Property names of a JSON schema as a nested dict, through array items."""
    if "items" in schema:
        return schema_tree(schema["items"])
    if "properties" in schema:
        return {name: schema_tree(sub) for name, sub in schema["properties"].items()}
    return None


class TestOrderRefundsShippingLines(unittest.TestCase):
    def setUp(self):
        for patcher in (
            patch.object(Context, "config", {"start_date": "2026-01-01T00:00:00Z"}),
            patch.object(Context, "state", {}),
            patch("singer.write_state"),
            patch("tap_shopify.streams.order_refunds.utils.now",
                  return_value=datetime(2026, 1, 2, tzinfo=timezone.utc)),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.stream = OrderRefunds()

    @patch.object(OrderRefunds, "call_api")
    @patch.object(Context, "get_unselected_fields", return_value=[])
    def test_selected_field_fetches_shipping_lines_for_synced_refunds(self, _unselected, call_api):
        old = refund(3, updated_at="2025-12-31T00:00:00Z")
        call_api.side_effect = [
            orders_page(refund(1), refund(2), old),
            nodes_response({
                "gid://shopify/Refund/1": [shipping_line_node(10)],
                "gid://shopify/Refund/2": [],
            }),
        ]

        records = list(self.stream.get_objects())

        self.assertEqual([r["id"] for r in records],
                         ["gid://shopify/Refund/1", "gid://shopify/Refund/2"])
        self.assertEqual(records[0]["refundShippingLines"], [shipping_line_node(10)])
        self.assertEqual(records[1]["refundShippingLines"], [])
        self.assertEqual(call_api.call_count, 2)
        call_api.assert_called_with(
            {"ids": ["gid://shopify/Refund/1", "gid://shopify/Refund/2"]},
            query=REFUND_SHIPPING_LINES_QUERY, data_key="nodes")

    @patch.object(OrderRefunds, "call_api")
    @patch.object(Context, "get_unselected_fields", return_value=["refundShippingLines"])
    def test_unselected_field_makes_no_extra_call(self, _unselected, call_api):
        call_api.side_effect = [orders_page(refund(1))]

        records = list(self.stream.get_objects())

        self.assertEqual(records, [refund(1)])
        self.assertEqual(call_api.call_count, 1)

    @patch.object(OrderRefunds, "call_api")
    @patch.object(Context, "get_unselected_fields", return_value=[])
    def test_page_without_new_refunds_makes_no_extra_call(self, _unselected, call_api):
        call_api.side_effect = [orders_page(refund(3, updated_at="2025-12-31T00:00:00Z"))]

        self.assertEqual(list(self.stream.get_objects()), [])
        self.assertEqual(call_api.call_count, 1)

    @patch.object(OrderRefunds, "call_api")
    def test_ids_are_batched_by_nodes_limit(self, call_api):
        refunds = [refund(i) for i in range(MAX_NODES_PER_QUERY + 1)]
        call_api.return_value = [None]

        self.stream.add_refund_shipping_lines(refunds)

        batch_sizes = [len(call.args[0]["ids"]) for call in call_api.call_args_list]
        self.assertEqual(batch_sizes, [MAX_NODES_PER_QUERY, 1])
        self.assertTrue(all(r["refundShippingLines"] == [] for r in refunds))

    @patch.object(OrderRefunds, "call_api")
    def test_fetches_all_shipping_line_pages_per_refund(self, call_api):
        first_refund = "gid://shopify/Refund/1"
        second_refund = "gid://shopify/Refund/2"
        first_line = shipping_line_node(10)
        second_line = shipping_line_node(11)
        third_line = shipping_line_node(12)
        fourth_line = shipping_line_node(13)
        call_api.side_effect = [
            nodes_response(
                {first_refund: [first_line], second_refund: [third_line]},
                {
                    first_refund: {"hasNextPage": True, "endCursor": "refund-1-cursor"},
                    second_refund: {"hasNextPage": True, "endCursor": "refund-2-cursor"},
                },
            ),
            {"id": first_refund,
             "refundShippingLines": {
                 "edges": [{"node": second_line}],
                 "pageInfo": {"hasNextPage": True, "endCursor": "refund-1-next"},
             }},
            {"id": first_refund,
             "refundShippingLines": {
                 "edges": [],
                 "pageInfo": {"hasNextPage": False, "endCursor": None},
             }},
            {"id": second_refund,
             "refundShippingLines": {
                 "edges": [{"node": fourth_line}],
                 "pageInfo": {"hasNextPage": False, "endCursor": None},
             }},
        ]
        refunds = [refund(1), refund(2)]

        self.stream.add_refund_shipping_lines(refunds)

        self.assertEqual(refunds[0]["refundShippingLines"], [first_line, second_line])
        self.assertEqual(refunds[1]["refundShippingLines"], [third_line, fourth_line])
        self.assertEqual(call_api.call_count, 4)
        self.assertEqual(call_api.call_args_list[0].args[0],
                         {"ids": [first_refund, second_refund]})
        self.assertEqual(call_api.call_args_list[1].args[0],
                         {"id": first_refund, "after": "refund-1-cursor"})
        self.assertEqual(call_api.call_args_list[1].kwargs["query"],
                         REFUND_SHIPPING_LINES_PAGE_QUERY)
        self.assertEqual(call_api.call_args_list[2].args[0],
                         {"id": first_refund, "after": "refund-1-next"})
        self.assertEqual(call_api.call_args_list[3].args[0],
                         {"id": second_refund, "after": "refund-2-cursor"})

    def test_shipping_lines_query_matches_schema(self):
        query = field_tree(parse(REFUND_SHIPPING_LINES_QUERY).definitions[0].selection_set)
        schema = json.loads(SCHEMA_PATH.read_text())

        self.assertEqual(query["nodes"]["refundShippingLines"]["edges"]["node"],
                         schema_tree(schema["properties"]["refundShippingLines"]))
