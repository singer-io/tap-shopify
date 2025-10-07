from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class CustomCollections(Stream):
    """Stream class for Custom Collections in Shopify."""
    name = "custom_collections"
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
        extended_query = f"{base_query} AND collection_type:custom"
        params["query"] = extended_query

        return params

    def get_query(self):
        return """
        query CustomCollections($first: Int!, $after: String, $query: String) {
            collections(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        description
                        title
                        updatedAt
                        handle
                        descriptionHtml
                        sortOrder
                        templateSuffix
                        image {
                            height
                            width
                            altText
                            url
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

Context.stream_objects["custom_collections"] = CustomCollections
