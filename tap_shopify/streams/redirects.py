from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Redirects(Stream):
    """Stream class for Redirects in Shopify."""
    name = "redirects"
    data_key = "urlRedirects"
    replication_key = "createdAt"
    access_scope = ["read_online_store_navigation"]

    def transform_object(self, obj, **_kwargs):
        """
        If the replication key is missing in the input node, a fallback timestamp (current UTC time)
        is injected to allow the pipeline to continue operating in incremental sync mode.
        Ensures the replication key is returned as an ISO string.

        Args:
            obj (dict): Product object.
            **_kwargs: Optional additional parameters.
        Returns:
            dict: Transformed product object.
        """
        if self.replication_key not in obj or not obj[self.replication_key]:
            last_updated_at = _kwargs.get(
                "last_updated_at",
                utils.now().replace(microsecond=0)
            )
            obj[self.replication_key] = last_updated_at.isoformat()
        return obj

    def get_query(self):
        return """
        query Redirects($first: Int!, $after: String, $query: String) {
            urlRedirects(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        path
                        target
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["redirects"] = Redirects
