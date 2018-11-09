import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

Context.stream_objects['orders'] = Orders
