"""MetafieldsOrders stream for Shopify tap."""

from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsOrders(Metafields):
    """Stream class for metafields associated with orders."""
    name = "metafields_orders"
    data_key = "orders"

    def get_query(self):
        """Returns the GraphQL query for fetching order metafields."""
        return """
        query getOrderMetafields(
            $first: Int!, $after: String, $query: String, $childafter: String
        ) {
            orders(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        metafields(first: $first, after: $childafter) {
                            edges {
                                node {
                                    id
                                    ownerType
                                    value
                                    type
                                    key
                                    createdAt
                                    namespace
                                    description
                                    updatedAt
                                    owner {
                                        ... on Order {
                                            id
                                        }
                                    }
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                        id
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }"""


# Register the stream object in the context
Context.stream_objects["metafields_orders"] = MetafieldsOrders
