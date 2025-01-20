
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_inventory_items_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class InventoryItems(ShopifyGqlStream):
    name = 'inventory_items'
    data_key = "inventoryItems"
    replication_key = "updatedAt"

    def get_query(self):
        return get_inventory_items_query()

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor,):
        """
        Returns Query and pagination params for filtering
        """
        rkey = "updated_at"
        params = {
            "query": f"{rkey}:>='{updated_at_min}' AND {rkey}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def transform_object(self, obj):
        return obj

Context.stream_objects['inventory_items'] = InventoryItems
