import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class Products(Stream):
    name = 'products'
    replication_object = shopify.Product

Context.stream_objects['products'] = Products
