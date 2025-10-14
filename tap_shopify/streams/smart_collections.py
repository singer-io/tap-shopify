from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class SmartCollections(Stream):
    """Stream class for Smart Collections in Shopify."""
    name = "smart_collections"
    data_key = "collections"
    replication_key = "updatedAt"
    access_scope = ["read_products"]

    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Extend base query params by adding `collection_type:custom` to the query filter.

        This keeps the updated_at filtering from the base class and adds
        the supported filter for custom collections.

        Args:
            updated_at_min (str): Lower bound for updated_at filtering.
            updated_at_max (str): Upper bound for updated_at filtering.
            cursor (str): Pagination cursor.

        Returns:
            dict: Modified query parameters with only supported filters.
        """
        params = super().get_query_params(updated_at_min, updated_at_max, cursor)
        base_query = params.get("query", "")
        extended_query = f"{base_query} AND collection_type:smart"
        params["query"] = extended_query

        return params

    def get_query(self):
        return """
        query SmartCollections($first: Int!, $after: String, $query: String) {
            collections(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        handle
                        title
                        updatedAt
                        description
                        descriptionHtml
                        sortOrder
                        templateSuffix
                        ruleSet {
                            appliedDisjunctively
                            rules {
                                column
                                relation
                                condition
                            }
                        }
                        seo {
                            description
                            title
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

Context.stream_objects["smart_collections"] = SmartCollections
