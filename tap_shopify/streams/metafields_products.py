from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsProducts(Metafields):
    name = 'metafields_products'
    data_key = "products"

    def get_query(self):
        qry = """
            query getProductMetafields($first: Int!, $after: String, $query: String, $childafter: String) {
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
        return qry

Context.stream_objects['metafields_products'] = MetafieldsProducts
