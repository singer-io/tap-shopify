import shopify
from singer.utils import strftime,strptime_to_utc
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context
from singer import utils
import singer

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250
MAX_IDS_COUNT = 100

class InventoryItems(Stream):
    name = 'inventory_items'
    replication_object = shopify.InventoryItem

    @shopify_error_handling
    def get_inventory_items(self, list_of_ids):
        return self.replication_object.find(
            ids=list_of_ids,
            limit=RESULTS_PER_PAGE)

    def get_objects(self):
        selected_parent = Context.stream_objects['products']()
        selected_parent.name = "inventory_items_products"
        inventory_item_ids = set()

        # Page through all `products`, bookmarking at `inventory_items_products`
        for parent_object in selected_parent.get_objects():
            for variant in parent_object.variants:
                inventory_item_ids.add(variant.inventory_item_id)

        str_list_of_inventory_item_ids = [str(inventory_item_id) for inventory_item_id in inventory_item_ids]
        len_of_inventory_item_ids = len(str_list_of_inventory_item_ids)

        # count the number of iterations as max limit of ids is 100
        no_of_iteration = int(len_of_inventory_item_ids/MAX_IDS_COUNT)
        for iteration in range(no_of_iteration + 1):
            # 0-99, 100-199, ...
            list_of_ids = ",".join(str_list_of_inventory_item_ids[(iteration * MAX_IDS_COUNT):(iteration * MAX_IDS_COUNT)+MAX_IDS_COUNT])
            inventory_items = self.get_inventory_items(list_of_ids)
            for inventory_item in inventory_items:
                yield inventory_item
            
    
    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for inventory_item in self.get_objects():
            inventory_item_dict = inventory_item.to_dict()
            replication_value = strptime_to_utc(inventory_item_dict[self.replication_key])
            if replication_value >= bookmark:
                yield inventory_item_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(strftime(max_bookmark))

Context.stream_objects['inventory_items'] = InventoryItems