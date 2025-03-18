from tap_shopify.context import Context
from tap_shopify.streams.metafields import Metafields


class MetafieldsOrders(Metafields):
    name = 'metafields_orders'
    data_key = "orders"

    def get_query(self):
        qry = """
            query getProductMetafields($first: Int!, $after: String, $query: String, $childafter: String) {
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
                                ... on Customer {
                                id
                                }
                                ... on Product {
                                id
                                }
                                ... on Order {
                                id
                                }
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

Context.stream_objects['metafields_orders'] = MetafieldsOrders
