import shopify

from tap_shopify.context import Context

from tap_shopify.streams.graph_ql_stream import GraphQlChildStream
import json


class OrderRefunds(GraphQlChildStream):
    name = 'order_refunds'
    replication_object = shopify.Refund
    parent_key_access = "refunds"
    parent_name = "orders"
    parent_id_ql_prefix = 'gid://shopify/Order/'
    child_per_page = 100
    parent_per_page = 25
    need_edges_cols = ["refundLineItems"]


Context.stream_objects['order_refunds'] = OrderRefunds
