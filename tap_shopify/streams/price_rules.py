import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class PriceRules(Stream):
    name = 'price_rules'
    replication_object = shopify.PriceRule

Context.stream_objects['price_rules'] = PriceRules
