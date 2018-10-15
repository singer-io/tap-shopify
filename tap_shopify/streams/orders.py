import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    def sync(self):
        for order in self.get_objects():
            order_dict = order.to_dict()
            # Popping this because Transactions is its own stream
            order_dict.pop("transactions", None)
            # Popping this because metafields is its own stream
            order_dict.pop("metafields", None)
            yield order_dict

Context.stream_objects['orders'] = Orders
