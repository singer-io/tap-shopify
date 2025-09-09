from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Blogs(Stream):
    """Stream class for Blogs in Shopify."""
    name = "blogs"
    data_key = "blogs"
    replication_key = "updatedAt"

    def get_query(self):
        return """
        query Blogs($first: Int!, $after: String, $query: String) {
            blogs(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        handle
                        commentPolicy
                        createdAt
                        tags
                        updatedAt
                        title
                        templateSuffix
                        articles(first: 250) {
                            edges {
                                node {
                                    body
                                    id
                                    title
                                    updatedAt
                                    publishedAt
                                    tags
                                    isPublished
                                    createdAt
                                }
                            }
                        }
                        feed {
                            location
                            path
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

Context.stream_objects["blogs"] = Blogs
