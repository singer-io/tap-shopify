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
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def transform_object(self, obj):
        hsc = obj.get("countryHarmonizedSystemCodes")
        hsc_list  = []
        if hsc and "edges" in hsc:
            for edge in hsc.get("edges"):
                node = edge.get("node")
                if node:
                    hsc_list.append(node)
        obj["countryHarmonizedSystemCodes"] = hsc_list
        return obj

Context.stream_objects['inventory_items'] = InventoryItems
