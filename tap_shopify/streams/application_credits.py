from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream


class ApplicationCredits(FullTableStream):
    """Stream class for Application Credits in Shopify."""
    name = "application_credits"
    data_key = "appInstallation"

    def call_api(self, query_params, query=None, data_key=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=data_key)
        data = (
            root_data
            .get("credits", {})
        )
        return data

    def get_query(self):
        return """
        query ApplicationCredits($first: Int!, $after: String) {
            appInstallation {
                credits(first: $first, after: $after) {
                    edges {
                        node {
                            amount {
                                amount
                                currencyCode
                            }
                            createdAt
                            description
                            id
                            test
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

Context.stream_objects["application_credits"] = ApplicationCredits
