import singer
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class Policies(Stream):
    """Stream class for Policies in Shopify."""
    name = "policies"
    data_key = "shop"
    replication_key = "updatedAt"

    def call_api(self, query_params, query=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=self.data_key)
        data = (
            root_data
            .get("shopPolicies", {})
        )
        return data

    def transform_object(self, obj):
        """
        If the replication key is missing in the input node, a fallback timestamp (current UTC time)
        is injected to allow the pipeline to continue operating in incremental sync mode.
        Ensures the replication key is returned as an ISO string.

        Args:
            obj (dict): Product object.

        Returns:
            dict: Transformed product object.
        """
        if self.replication_key not in obj or not obj[self.replication_key]:
            obj[self.replication_key] = utils.now().replace(microsecond=0).isoformat()
        return obj

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

        query_params = {}

        with metrics.http_request_timer(self.name):
            data = self.call_api(query_params, query=query)

        for record in data:
            obj = self.transform_object(record)
            yield obj

    def get_query(self):
        return """
        query marketingEvents {
            shop {
                shopPolicies {
                    body
                    createdAt
                    id
                    title
                    type
                    updatedAt
                    url
                }
            }
        }
        """

Context.stream_objects["policies"] = Policies
