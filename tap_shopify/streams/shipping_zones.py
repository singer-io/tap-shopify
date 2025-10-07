from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream

RESULTS_PER_PAGE = 10

class ShippingZones(FullTableStream):
    """Stream class for Shipping Zones in Shopify."""
    name = "shipping_zones"
    data_key = "deliveryProfiles"
    access_scope = ["read_shipping"]

    def __init__(self):
        super().__init__()
        self.results_per_page = RESULTS_PER_PAGE

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
