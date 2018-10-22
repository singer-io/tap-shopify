import singer
import shopify
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE)
from tap_shopify.context import Context


class Collects(Stream):
    name = 'collects'
    replication_object = shopify.Collect
    replication_key = 'id'

    def get_objects(self):
        page = 1
        start_id = singer.get_bookmark(Context.state,
                                       self.name,
                                       self.replication_key)

        while True:
            query_params = {
                "page": page,
                "limit": RESULTS_PER_PAGE,
            }
            if start_id:
                query_params["since_id"] = start_id

            objects = self.call_api(query_params)

            for obj in objects:
                yield obj
                self.update_bookmark(obj)

            if len(objects) < RESULTS_PER_PAGE:
                break
            page += 1


Context.stream_objects['collects'] = Collects
