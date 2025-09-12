from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream


class CarrierServices(FullTableStream):
    """Stream class for Carrier Services in Shopify."""
    name = "carrier_services"
    data_key = "carrierServices"

    def get_query(self):
        return """
        query CarrierServices($first: Int!, $after: String, $query: String) {
            carrierServices(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        name
                        active
                        callbackUrl
                        formattedName
                        supportsServiceDiscovery
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["carrier_services"] = CarrierServices
