import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class ArticleTags(Stream):
    """Stream class for Article Tags in Shopify."""
    name = "article_tags"
    data_key = "articleTags"

    # pylint: disable=W0221
    def get_query_params(self):
        """
        Construct query parameters for GraphQL requests.

        Args:
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        params = {
            "limit": 250,
        }
        return params

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Transformation
        """
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        query_params = self.get_query_params()

        with metrics.http_request_timer(self.name):
            data = self.call_api(query_params, query=query)
            for record in data:
                obj = self.transform_object(record)
                yield obj

    def get_query(self):
        return """
        query ArticleTags($limit: Int!) {
            articleTags(limit: $limit)
        }
        """

Context.stream_objects["article_tags"] = ArticleTags
