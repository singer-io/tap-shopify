"""MetafieldsCollections stream for Shopify tap."""

from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsCollections(Metafields):
    """Stream class for metafields associated with collections."""

    name = "metafields_collections"
    data_key = "collections"

    def get_query(self):
        """Return the GraphQL query for fetching collection metafields."""
        return """
            query getCollectionsMetafields(
                $first: Int!, $after: String, $query: String, $childafter: String
            ) {
                collections(first: $first, after: $after, query: $query) {
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
                                            ... on Collection {
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


Context.stream_objects["metafields_collections"] = MetafieldsCollections
