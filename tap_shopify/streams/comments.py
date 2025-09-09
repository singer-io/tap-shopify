from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Comments(Stream):
    """Stream class for Comments in Shopify."""
    name = "comments"
    data_key = "comments"
    replication_key = "updatedAt"

    def get_query(self):
        return """
        query Comments($first: Int!, $after: String, $query: String) {
            comments(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        body
                        bodyHtml
                        author {
                            email
                            name
                        }
                        createdAt
                        updatedAt
                        userAgent
                        status
                        publishedAt
                        isPublished
                        ip
                        article {
                            id
                            title
                            tags
                            publishedAt
                            isPublished
                            createdAt
                            body
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

Context.stream_objects["comments"] = Comments
