from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream


class ArticleAuthors(FullTableStream):
    """Stream class for Article Authors in Shopify."""
    name = "article_authors"
    data_key = "articleAuthors"
    key_properties = ["name"]
    access_scope = ["read_content" ,"read_online_store_pages"]

    def get_query(self):
        return """
        query ArticleAuthors($first: Int!, $after: String) {
            articleAuthors(first: $first, after: $after) {
                edges {
                    node {
                        name
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["article_authors"] = ArticleAuthors
