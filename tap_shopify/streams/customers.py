import shopify
from tap_shopify.streams.base import Stream, RunAsync
from tap_shopify.context import Context


class Customers(Stream):
    name = 'customers'
    replication_object = shopify.Customer
    endpoint = "/customers"
    result_key = "customers"
    async_available = True

Context.stream_objects['customers'] = Customers
