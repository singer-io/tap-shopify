import singer
import shopify
from singer.utils import strftime,strptime_to_utc
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

class InventoryItems(Stream):
    name = 'inventory_items'
    replication_object = shopify.InventoryItem

    @shopify_error_handling
    def get_inventory_items(self, inventory_items_ids):
        return self.replication_object.find(
            ids=inventory_items_ids,
            limit=RESULTS_PER_PAGE)

    def get_objects(self):

        selected_parent = Context.stream_objects['products']()
        selected_parent.name = "product_variants"

        # Page through all `products`, bookmarking at `product_variants`
        for parent_object in selected_parent.get_objects():

            product_variants = parent_object.variants
            inventory_items_ids = ",".join(
                [str(product_variant.inventory_item_id) for product_variant in product_variants])

            # Max limit of IDs is 100 and Max limit of product_variants in one product is also 100
            # hence we can directly pass all inventory_items_ids
            inventory_items = self.get_inventory_items(inventory_items_ids)

            for inventory_item in inventory_items:
                yield inventory_item

    def sync(self):
        for inventory_item in self.get_objects():
            yield inventory_item.to_dict()

Context.stream_objects['inventory_items'] = InventoryItems
