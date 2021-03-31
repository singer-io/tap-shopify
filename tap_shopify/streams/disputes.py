import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Disputes(Stream):
    name = 'disputes'
    replication_object = shopify.Disputes

Context.stream_objects['disputes'] = Disputes
