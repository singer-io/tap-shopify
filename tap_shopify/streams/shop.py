import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class Shop(Stream):
    name = 'shop'
    replication_object = shopify.Shop

    def sync(self):
        response = shopify.Shop.current()
        return [response.to_dict()]

Context.stream_objects['shop'] = Shop