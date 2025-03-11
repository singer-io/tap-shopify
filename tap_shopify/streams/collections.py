from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream



class Collections(ShopifyGqlStream):
    name = 'collections'
    data_key = "collections"
    replication_key = "updatedAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params


    def transform_object(self, obj):
        obj["collections_type"] = "SMART" if obj.get("ruleSet") else "MANUAL"
        # TODO Process Products
        # obj["products"] = 
        return obj

    def get_query(self):
        qry = """query Collections($first: Int!, $after: String, $query: String) {
            collections(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                edges {
                    node {
                        id
                        title
                        handle
                        updatedAt
                        productsCount {
                            count
                            precision
                        }
                        sortOrder
                        ruleSet {
                            appliedDisjunctively
                            rules {
                                column
                                condition
                                relation
                            }
                        }
                        seo {
                            description
                            title
                        }
                        feedback {
                            summary
                        }
                        products(first: 250, sortKey: ID) {
                            edges {
                                node {
                                    id
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                }
            }
        }"""
        return qry

Context.stream_objects['collections'] = Collections
