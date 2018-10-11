import shopify
import singer
from  pyactiveresource.connection import ServerError
from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE

LOGGER = singer.get_logger()

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    replication_object = shopify.AbandonedCheckouts
    key_properties = ['id']

    def sync(self):
        for abandoned_checkout in self.get_objects():
            # TODO: Filter out customer and replace with ID? It can be foreign keyed
            yield abandoned_checkout.to_dict()

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
