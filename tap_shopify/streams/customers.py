import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context

#Consumers -> Base.py
class Customers(Stream):
    name = 'customers'
    replication_object = shopify.Customer

Context.stream_objects['customers'] = Customers
