from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream



class Locations(ShopifyGqlStream):
    name = 'locations'
    data_key = "locations"
    replication_key = "createdAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "created_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def get_query(self):
        qry = """query GetLocations($first: Int!, $after: String, $query: String) {
                locations(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                    node {
                        address {
                        countryCode
                        address1
                        city
                        address2
                        provinceCode
                        zip
                        province
                        phone
                        country
                        formatted
                        latitude
                        longitude
                        }
                        name
                        id
                        updatedAt
                        createdAt
                        isActive
                        addressVerified
                        deactivatable
                        deactivatedAt
                        deletable
                        fulfillsOnlineOrders
                        hasActiveInventory
                        hasUnfulfilledOrders
                        isFulfillmentService
                        legacyResourceId
                        localPickupSettingsV2 {
                        instructions
                        pickupTime
                        }
                        shipsInventory
                    }
                    }
                    pageInfo {
                    endCursor
                    hasNextPage
                    }
                }
                }"""
        return qry

Context.stream_objects['locations'] = Locations
