import shopify
import singer

from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE

# FIXME Most of these classes probably don't need LOGGER anymore
LOGGER = singer.get_logger()

class Orders(Stream):
    # FIXME remove unnecessary overrides
    name = 'orders'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    replication_object = shopify.Order
    key_properties = ['id']

    def sync(self):
        for order in self.get_objects():
            order_dict = order.to_dict()
            # Popping this because Transactions is its own stream
            order_dict.pop("transactions", None)
            # Popping this because metafields is its own stream
            order_dict.pop("metafields", None)
            yield order_dict

Context.stream_objects['orders'] = Orders
