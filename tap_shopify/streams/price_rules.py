from tap_shopify.context import Context
from tap_shopify.streams.base import FullTableStream


class PriceRules(FullTableStream):
    """Stream class for Price Rules in Shopify."""
    name = "price_rules"
    data_key = "discountNodes"
    access_scope = ["read_discounts"]

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
