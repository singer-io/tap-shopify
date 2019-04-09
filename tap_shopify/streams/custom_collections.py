import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class CustomCollections(Stream):
    name = 'custom_collections'
    replication_object = shopify.CustomCollection

Context.stream_objects['custom_collections'] = CustomCollections
