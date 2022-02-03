import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context


class Events(Stream):
    name = 'events'
    replication_object = shopify.Event
    replication_key = "created_at"

    def get_query_params(self, since_id, status_key, updated_at_min, updated_at_max):
        return {
            "since_id": since_id,
            "created_at_min": updated_at_min,
            "created_at_max": updated_at_max,
            "limit": self.results_per_page,
            status_key: "any"
        }

Context.stream_objects['events'] = Events
