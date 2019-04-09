import shopify
from tap_shopify.streams.base import Stream, RunAsync
from tap_shopify.context import Context


class Products(Stream):
    name = 'products'
    replication_object = shopify.Product
    endpoint = "/products"
    result_key = "products"
    async_available = True

Context.stream_objects['products'] = Products
