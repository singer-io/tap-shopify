from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class ScriptTags(Stream):
    """Stream class for Script Tags in Shopify."""
    name = "script_tags"
    data_key = "scriptTags"
    replication_key = "updatedAt"
    access_scope = ["read_script_tags"]

    def get_query(self):
        return """
        query ScriptTags($first: Int!, $after: String, $query: String) {
            scriptTags(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        cache
                        createdAt
                        updatedAt
                        displayScope
                        legacyResourceId
                        src
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["script_tags"] = ScriptTags
