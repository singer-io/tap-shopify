import shopify
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      OutOfOrderIdsError)
from tap_shopify.context import Context


class Redirects(Stream):
    name = 'redirects'
    replication_object = shopify.Redirect
    # Redirects have no timestamps, but can be updated after creation
    # So the only option is to use full table replication
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    replication_key = None

    def get_objects(self):
        # Override base function as this is a full sync every time
        since_id = 1
        while True:
            query_params = {
                "since_id": since_id,
                "limit": RESULTS_PER_PAGE,
            }

            objects = self.call_api(query_params)

            for obj in objects:
                if obj.id < since_id:
                    raise OutOfOrderIdsError("obj.id < since_id: {} < {}".format(
                        obj.id, since_id))
                yield obj

            if len(objects) < RESULTS_PER_PAGE:
                break
            if objects[-1].id != max([o.id for o in objects]):
                raise OutOfOrderIdsError("{} is not the max id in objects ({})".format(
                    objects[-1].id, max([o.id for o in objects])))
            since_id = objects[-1].id


Context.stream_objects['redirects'] = Redirects
