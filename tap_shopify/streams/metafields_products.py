"""MetafieldsProducts stream for Shopify tap."""

from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsProducts(Metafields):
    """Stream class for product metafields."""
    name = "metafields_products"
    data_key = "products"

    def get_query(self):
        """Return the GraphQL query for product metafields."""
        query = """
        query getProductMetafields(
            $first: Int!, $after: String, $query: String, $childafter: String
        ) {
            products(first: $first, after: $after, query: $query) {
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
                                        ... on Product {
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
        return query


Context.stream_objects["metafields_products"] = MetafieldsProducts
