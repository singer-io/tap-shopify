import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Disputes(Stream):
    name = 'disputes'
    replication_object = shopify.Disputes

    def get_query_params(self, since_id, updated_at_min, updated_at_max, results_per_page):
        return {
            "since_id": since_id,
            "initiated_at": updated_at_min
        }

Context.stream_objects['disputes'] = Disputes
