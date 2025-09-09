import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class Currencies(Stream):
    """Stream class for Currencies in Shopify."""
    name = "currencies"
    data_key = "shop"

    def call_api(self, query_params, query=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=self.data_key)
        data = (
            root_data
            .get("currencySettings", {})
        )
        return data

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
            - Transformation
        """
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        has_next_page, cursor = True, None

        while has_next_page:
            query_params = self.get_query_params(cursor)

            with metrics.http_request_timer(self.name):
                data = self.call_api(query_params, query=query)

            for edge in data.get("edges"):
                obj = self.transform_object(edge.get("node"))
                yield obj

            page_info =  data.get("pageInfo")
            cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")


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
