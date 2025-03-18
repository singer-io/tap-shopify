from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsCollections(Metafields):
    name = 'metafields_collections'
    data_key = "collections"

    def get_query(self):
        qry = """
            query getProductMetafields($first: Int!, $after: String, $query: String, $childafter: String) {
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
                }"""
        return qry

Context.stream_objects['metafields_collections'] = MetafieldsCollections
