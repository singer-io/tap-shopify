import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class Giftcards(Stream):
    name = 'giftcards'
    replication_object = shopify.GiftCard


Context.stream_objects['giftcards'] = Giftcards
