import shopify
from singer.utils import strftime, strptime_to_utc
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)
from tap_shopify.context import Context

class InventoryLevels(Stream):
    name = 'inventory_levels'
    replication_key = 'updated_at'
    key_properties = ['location_id', 'inventory_item_id']
    replication_object = shopify.InventoryLevel
    # Added decorator over functions of shopify SDK
    replication_object.find = shopify_error_handling(replication_object.find)

    def api_call_for_inventory_levels(self, parent_object_id, bookmark):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        return self.replication_object.find(
            updated_at_min = bookmark,
            limit = RESULTS_PER_PAGE,
            location_ids=parent_object_id
        )

    def get_inventory_levels(self, parent_object, bookmark):
        inventory_page = self.api_call_for_inventory_levels(parent_object, bookmark)
        yield from inventory_page

        while inventory_page.has_next_page():
            inventory_page = inventory_page.next_page()
            yield from inventory_page

    def get_objects(self):
        bookmark = self.get_bookmark()

        selected_parent = Context.stream_objects['locations']()
        selected_parent.name = "inventory_level_locations"

        # Get all locations data as location id is used for Inventory Level
        # If we get locations updated after a bookmark
        # then there is possibility of data loss for Inventory Level
        # because location is not updated when any Inventory Level is updated inside it.
        for parent_object in selected_parent.get_locations_data():
            yield from self.get_inventory_levels(parent_object.id, bookmark)

    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for inventory_level in self.get_objects():
            inventory_level_dict = inventory_level.to_dict()
            replication_value = strptime_to_utc(inventory_level_dict[self.replication_key])
            if replication_value >= bookmark:
                yield inventory_level_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(strftime(max_bookmark))

Context.stream_objects['inventory_levels'] = InventoryLevels
