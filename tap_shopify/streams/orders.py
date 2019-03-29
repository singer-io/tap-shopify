import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RunAsync


class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order
    endpoint = "/orders"
    result_key = "orders"
    async_available = True

Context.stream_objects['orders'] = Orders
