import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()
RESULTS_PER_PAGE = 10

class ShippingZones(Stream):
    """Stream class for Shipping Zones in Shopify."""
    name = "shipping_zones"
    data_key = "deliveryProfiles"

    # pylint: disable=W0221
    def get_query_params(self, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        params = {
            "first": RESULTS_PER_PAGE,
        }

        if cursor:
            params["after"] = cursor
        return params

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Pagination & Filtering of stream
            - Transformation
        """
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        has_next_page, cursor = True, None

        while has_next_page:
            query_params = self.get_query_params(cursor)

            with metrics.http_request_timer(self.name):
                data = self.call_api(query_params, query=query)

            for edge in data.get("edges"):
                obj = self.transform_object(edge.get("node"))
                yield obj

            page_info =  data.get("pageInfo")
            cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

    def get_query(self):
        return """
        query ShippingZones($first: Int!, $after: String) {
            deliveryProfiles(first: $first, after: $after) {
                edges {
                    node {
                        id
                        profileLocationGroups {
                            locationGroup {
                                id
                            }
                            locationGroupZones(first: 50) {
                                edges {
                                    node {
                                        zone {
                                            id
                                            name
                                            countries {
                                                code {
                                                    countryCode
                                                    restOfWorld
                                                }
                                                provinces {
                                                    name
                                                    code
                                                }
                                            }
                                        }
                                        methodDefinitions(first: 50) {
                                            edges {
                                                node {
                                                    id
                                                    active
                                                    description
                                                    methodConditions {
                                                        field
                                                        operator
                                                        conditionCriteria {
                                                            __typename
                                                            ... on MoneyV2 {
                                                                amount
                                                                currencyCode
                                                            }
                                                            ... on Weight {
                                                                unit
                                                                value
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["shipping_zones"] = ShippingZones
