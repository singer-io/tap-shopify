"""MetafieldsCustomers stream for Shopify tap."""

from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsCustomers(Metafields):
    """Stream class for metafields associated with customers."""

    name = "metafields_customers"
    data_key = "customers"

    def get_query(self):
        """Return the GraphQL query for fetching customer metafields."""
        return """
            query getCustomerMetafields(
                $first: Int!, $after: String, $query: String, $childafter: String
            ) {
                customers(first: $first, after: $after, query: $query) {
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
                                            ... on Customer {
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
            }
        """

Context.stream_objects["metafields_customers"] = MetafieldsCustomers
