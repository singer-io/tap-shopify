import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RunAsync

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout
    endpoint = "/checkouts"
    result_key = "checkouts"
    async_available = True

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
