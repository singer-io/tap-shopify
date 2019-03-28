import shopify
from tap_shopify.streams.base import Stream, RunAsync
from tap_shopify.context import Context


class Customers(Stream):
    name = 'customers'
    replication_object = shopify.Customer
    replication_object_async = RunAsync
    endpoint = "/customers"
    result_key = "customers"

Context.stream_objects['customers'] = Customers
