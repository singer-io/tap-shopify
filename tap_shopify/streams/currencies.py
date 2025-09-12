from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream


class Currencies(FullTableStream):
    """Stream class for Currencies in Shopify."""
    name = "currencies"
    data_key = "shop"
    key_properties = ["currencyCode"]

    def call_api(self, query_params, query=None, data_key=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=data_key)
        data = (
            root_data
            .get("currencySettings", {})
        )
        return data

    def get_query(self):
        return """
        query Currencies($first: Int!, $after: String) {
            shop {
                currencySettings(first: $first, after: $after) {
                    edges {
                        node {
                            currencyCode
                            currencyName
                            enabled
                            manualRate
                            rateUpdatedAt
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        }
        """

Context.stream_objects["currencies"] = Currencies
