import singer
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class Themes(Stream):
    """Stream class for Themes in Shopify."""
    name = "themes"
    data_key = "themes"
    replication_key = "updatedAt"
    access_scope = ["read_themes"]

    # pylint: disable=W0221
    def get_query_params(self, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        params = {
            "first": self.results_per_page,
        }

        if cursor:
            params["after"] = cursor
        return params

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Pagination & Filtering of stream
            - Transformation and bookmarking
        """
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))
        has_next_page, cursor = True, None

        while has_next_page:
            query_params = self.get_query_params(cursor)

            with metrics.http_request_timer(self.name):
                data = self.call_api(query_params, query=query)

            for edge in data.get("edges"):
                obj = self.transform_object(edge.get("node"))
                replication_value = utils.strptime_to_utc(obj[self.replication_key])
                if replication_value >= current_bookmark:
                    current_bookmark = max(current_bookmark, replication_value)
                    yield obj

            page_info = data.get("pageInfo")
            cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")
            self.update_bookmark(utils.strftime(current_bookmark))

    def get_query(self):
        return """
        query Themes($first: Int!, $after: String) {
            themes(first: $first, after: $after) {
                edges {
                    node {
                        id
                        name
                        prefix
                        processing
                        processingFailed
                        role
                        themeStoreId
                        updatedAt
                        createdAt
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["themes"] = Themes
