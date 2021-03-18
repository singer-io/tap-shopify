import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE
from tap_shopify.streams.child_stream import ChildStream

import singer

LOGGER = singer.get_logger()


class InventoryLevel(ChildStream):
    name = 'inventory_levels'
    replication_object = shopify.InventoryLevel
    replication_method = 'FULL_TABLE'

    def get_parent_field_name(self):
        return "location_ids"

    def get_parent_name(self):
        return "locations"

    def get_objects(self):
        selected_parent = Context.stream_objects[self.get_parent_name()]()
        selected_parent.name = self.name

        # Page through all `orders`, bookmarking at `child_orders`
        LOGGER.info("Getting data")
        for parent_object in selected_parent.get_objects():
            children = self.get_children(parent_object, None, include_since_id=False)
            while True:
                for child in children:
                    yield child
                if len(children) < RESULTS_PER_PAGE:
                    break
                children = self.get_next_page(children)


Context.stream_objects['inventory_levels'] = InventoryLevel
