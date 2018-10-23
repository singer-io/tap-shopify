import shopify
from singer import utils
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE)
from tap_shopify.context import Context


class Collects(Stream):
    name = 'collects'
    replication_object = shopify.Collect
    replication_key = 'updated_at'

    def get_objects(self):
        page = 1
        bookmark = self.get_bookmark()
        max_bookmark = utils.strftime(utils.now())
        while True:
            query_params = {
                "page": page,
                "limit": RESULTS_PER_PAGE,
            }

            objects = self.call_api(query_params)

            for obj in objects:
                # Syncing Collects is a full sync every time but emitting records that have
                # an updated_date greater than the bookmark
                if utils.strptime_with_tz(obj.updated_at) > bookmark:
                    yield obj

            if len(objects) < RESULTS_PER_PAGE:
                # Update the bookmark at the end of the last page
                self.update_bookmark(max_bookmark)
                break
            page += 1


Context.stream_objects['collects'] = Collects
