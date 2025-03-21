from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class Locations(ShopifyGqlStream):
    """Stream class for Shopify Locations"""

    name = "locations"
    data_key = "locations"
    replication_key = "createdAt"

    # pylint: disable=arguments-differ
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query parameters for filtering and pagination.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str, optional): Pagination cursor.

        Returns:
            dict: Query parameters.
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
