import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    # If we ever needed to optimize network requests, we think we
    # would drop the formal substream objects and do all substream
    # syncing here.
    def sync(self):
        for order in self.get_objects():
            order_dict = order.to_dict()
            # Popping this because Transactions is its own stream
            order_dict.pop("transactions", None)
            # Popping this because metafields is its own stream
            order_dict.pop("metafields", None)
            # Popping this because order_refunds is its own stream
            order_dict.pop("refunds", None)
            yield order_dict

Context.stream_objects['orders'] = Orders
