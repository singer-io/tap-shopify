import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class Events(Stream):
    name = 'events'
    replication_object = shopify.Event
    replication_key = "created_at"

Context.stream_objects['events'] = Events
