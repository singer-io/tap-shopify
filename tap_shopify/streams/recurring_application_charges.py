import singer
from singer import metrics
from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream

LOGGER = singer.get_logger()


class RecurringApplicationCharges(FullTableStream):
    """Stream class for Recurring Application Charges in Shopify."""
    name = "recurring_application_charges"
    data_key = "currentAppInstallation"

    def call_api(self, query_params, query=None, data_key=None):
        """
        Overriding call_api method to extract data from nested data_key.
        """
        root_data = super().call_api(query_params, query=query, data_key=data_key)
        data = (
            root_data
            .get("activeSubscriptions", {})
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
        query RecurringApplicationCharges {
            currentAppInstallation {
                activeSubscriptions {
                    createdAt
                    currentPeriodEnd
                    id
                    name
                    returnUrl
                    status
                    test
                    trialDays
                    lineItems {
                        id
                        plan {
                            pricingDetails {
                                __typename
                                ... on AppRecurringPricing {
                                    planHandle
                                    interval
                                    discount {
                                        durationLimitInIntervals
                                        remainingDurationInIntervals
                                        value {
                                            ... on AppSubscriptionDiscountAmount {
                                                __typename
                                                amount {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                            ... on AppSubscriptionDiscountPercentage {
                                                __typename
                                                percentage
                                            }
                                        }
                                        priceAfterDiscount {
                                            amount
                                            currencyCode
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

Context.stream_objects["recurring_application_charges"] = RecurringApplicationCharges
