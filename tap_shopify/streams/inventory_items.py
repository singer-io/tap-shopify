
from tap_shopify.context import Context
from tap_shopify.streams.graphql.gql_queries import get_inventory_items_query
from tap_shopify.streams.graphql.gql_base import ShopifyGqlStream



class InventoryItems(ShopifyGqlStream):
    name = 'inventory_items'
    data_key = "inventoryItems"
    replication_key = "updated_at"

    get_query = get_inventory_items_query

    def transform_object(self, obj):
        """
        performs compatibility transformations
        TODO: Check Descrepancy in response data
        TODO: Error Handling
        """
        obj["admin_graphql_api_id"] = obj["id"]
        obj["id"] = int(obj["id"].replace("gid://shopify/InventoryItem/", ""))
        obj["created_at"] = obj.get("createdAt")
        obj["updated_at"] = obj.get("updatedAt")
        obj["cost"] = (obj.get("unitCost") or {}).get("amount")
        obj["requires_shipping"] = obj.get("requiresShipping",)
        obj["country_code_of_origin"] = obj.get("countryCodeOfOrigin")
        obj["province_code_of_origin"] = obj.get("provinceCodeOfOrigin")

        # TODO: Test this
        country_harmonized_system_codes = []
        for edge in obj["countryHarmonizedSystemCodes"]["edges"]:
            item = edge.get("node") or {}
            itx = {}
            itx["harmonized_system_code"] = item.get("harmonizedSystemCode", None)
            itx["country_code"] = item.get("countryCode", None)
            country_harmonized_system_codes.append(item)
        obj["country_harmonized_system_codes"] = country_harmonized_system_codes
        obj["harmonized_system_code"] = obj[ "harmonizedSystemCode"]

        return obj

Context.stream_objects['inventory_items'] = InventoryItems
