import shopify

from tap_shopify.context import Context

from tap_shopify.streams.graph_ql_stream import GraphQlChildStream


class OrderRefunds(GraphQlChildStream):
    name = 'order_refunds'
    replication_object = shopify.Refund
    parent_key_access = "refunds"
    parent_name = "orders"


Context.stream_objects['order_refunds'] = OrderRefunds
