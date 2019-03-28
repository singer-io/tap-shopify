import shopify
from tap_shopify.streams.base import Stream, RunAsync
from tap_shopify.context import Context


class Products(Stream):
    name = 'products'
    replication_object = shopify.Product
    replication_object_async = RunAsync
    endpoint = "/products"
    result_key = "products"

Context.stream_objects['products'] = Products
