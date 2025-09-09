from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class PriceRules(Stream):
    """Stream class for Price Rules in Shopify."""
    name = "price_rules"
    data_key = "discountNodes"
    replication_key = "createdAt"

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

    def get_query(self):
        return """
        query PriceRules($first: Int!, $after: String, $query: String) {
            discountNodes(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        discount {
                            ... on DiscountAutomaticApp {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountAutomaticBasic {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountAutomaticBxgy {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                usesPerOrderLimit
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountAutomaticFreeShipping {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountCodeApp {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                usageLimit
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountCodeBasic {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                usageLimit
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountCodeBxgy {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                usageLimit
                                usesPerOrderLimit
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                            ... on DiscountCodeFreeShipping {
                                __typename
                                title
                                status
                                createdAt
                                startsAt
                                updatedAt
                                endsAt
                                usageLimit
                                combinesWith {
                                    orderDiscounts
                                    productDiscounts
                                    shippingDiscounts
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["price_rules"] = PriceRules
