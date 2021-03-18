import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE, shopify_error_handling
from tap_shopify.streams.child_stream import ChildStream

import singer

LOGGER = singer.get_logger()


class InventoryItem(ChildStream):
    name = 'inventory_items'
    replication_object = shopify.InventoryItem
    replication_method = 'FULL_TABLE'

    def get_parent_field_name(self):
        pass

    def get_parent_name(self):
        return "inventory_levels"

    def get_objects(self):
        selected_parent = Context.stream_objects[self.get_parent_name()]()
        selected_parent.name = self.name
        LOGGER.info("Getting chunk data")
        for chunk in self.get_chunks(selected_parent.get_objects()):
            children = self.get_children(",".join(chunk), None, include_since_id=False)
            for child in children:
                yield child

    def get_chunks(self, levels, chunk_size=50):
        chunk = []
        for level in levels:
            chunk.append(str(level.inventory_item_id))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if 0 < len(chunk) < chunk_size:
            yield chunk

    @shopify_error_handling
    def get_children(self, value, since_id, include_since_id=True):
        params = {
            "ids": value
        }
        return self.replication_object.find(**params)
