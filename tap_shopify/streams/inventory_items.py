from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream



class InventoryItems(ShopifyGqlStream):
    name = 'inventory_items'
    data_key = "inventoryItems"
    replication_key = "updatedAt"

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
    
    def get_query(self):
        """
        Returns GraphQL query to get all inventory items
        """
        return """
                query GetinventoryItems($first: Int!, $after: String, $query: String) {
                    inventoryItems(first: $first, after: $after, query: $query) {
                        edges {
                            node {
                                id
                                createdAt
                                sku
                                updatedAt
                                requiresShipping
                                countryCodeOfOrigin
                                provinceCodeOfOrigin
                                harmonizedSystemCode
                                tracked
                                unitCost {
                                    amount
                                }
                                countryHarmonizedSystemCodes(first: 175) {
                                    edges {
                                        node {
                                            countryCode
                                            harmonizedSystemCode
                                        }
                                    }
                                }
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            """

Context.stream_objects['inventory_items'] = InventoryItems
