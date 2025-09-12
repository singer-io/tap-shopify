import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream

LOGGER = singer.get_logger()


class FulfillmentServices(FullTableStream):
    """Stream class for Fulfillment Services in Shopify."""
    name = "fulfillment_services"
    data_key = "shop"

    def call_api(self, query_params, query=None, data_key=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=data_key)
        data = (
            root_data
            .get("fulfillmentServices", {})
        )
        return data

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
        query FulfillmentServices {
            shop {
                fulfillmentServices {
                    id
                    handle
                    callbackUrl
                    inventoryManagement
                    permitsSkuSharing
                    requiresShippingMethod
                    serviceName
                    trackingSupport
                    type
                }
            }
        }
        """

Context.stream_objects["fulfillment_services"] = FulfillmentServices
