from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Pages(Stream):
    """Stream class for Webhooks in Shopify."""
    name = "pages"
    data_key = "pages"
    replication_key = "updatedAt"

    def get_query(self):
        return """
        query Pages($first: Int!, $after: String, $query: String) {
            pages(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        updatedAt
                        handle
                        createdAt
                        bodySummary
                        body
                        isPublished
                        publishedAt
                        templateSuffix
                        title
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["pages"] = Pages
