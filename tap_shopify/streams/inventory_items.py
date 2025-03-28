from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class InventoryItems(Stream):
    """Stream class for inventory items."""

    name = "inventory_items"
    data_key = "inventoryItems"
    replication_key = "updatedAt"

    def transform_object(self, obj):
        """
        Transforms the object by extracting country harmonized system codes.

        Args:
            obj (dict): The object to transform.

        Returns:
            dict: Transformed object.
        """
        hsc = obj.get("countryHarmonizedSystemCodes")
        hsc_list = []
        if hsc and "edges" in hsc:
            for edge in hsc.get("edges"):
                node = edge.get("node")
                if node:
                    hsc_list.append(node)
        obj["countryHarmonizedSystemCodes"] = hsc_list
        return obj

    def get_query(self):
        """
        Returns GraphQL query to get all inventory items.

        Returns:
            str: GraphQL query string.
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


Context.stream_objects["inventory_items"] = InventoryItems
