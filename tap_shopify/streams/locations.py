from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class Locations(ShopifyGqlStream):
    """Stream class for Shopify Locations"""

    name = "locations"
    data_key = "locations"
    replication_key = "createdAt"

    def get_query(self):
        """
        Returns the GraphQL query for fetching locations.

        Returns:
            str: GraphQL query string.
        """
        return """
        query GetLocations($first: Int!, $after: String, $query: String) {
            locations(first: $first, after: $after, query: $query, sortKey: ID) {
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
        }
        """


Context.stream_objects["locations"] = Locations
