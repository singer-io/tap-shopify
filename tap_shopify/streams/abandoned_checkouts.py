import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
