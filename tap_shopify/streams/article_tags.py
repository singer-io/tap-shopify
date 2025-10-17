import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream

LOGGER = singer.get_logger()


class ArticleTags(FullTableStream):
    """Stream class for Article Tags in Shopify."""
    name = "article_tags"
    data_key = "articleTags"
    key_properties = ["article_tag"]
    access_scope = ["read_content" ,"read_online_store_pages"]

    # pylint: disable=W0221
    def get_query_params(self):
        """
        Construct query parameters for GraphQL requests.
        Returns:
            dict: Dictionary of query parameters.
        """
        params = {
            "limit": 250,
        }
        return params

    def transform_object(self, obj, **_kwargs):
        """
        Modify this to perform custom transformation on each object
        """
        return {"article_tag": obj}

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
