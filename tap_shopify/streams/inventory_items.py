
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_inventory_items_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class InventoryItems(ShopifyGqlStream):
    name = 'inventory_items'
    data_key = "inventoryItems"
    replication_key = "updatedAt"

    def get_query(self):
        return get_inventory_items_query()

    def transform_object(self, obj):
        """
        performs compatibility transformations
        """
        return obj

Context.stream_objects['inventory_items'] = InventoryItems
