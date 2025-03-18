from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class AbandonedCheckouts(ShopifyGqlStream):
    name = "abandoned_checkouts"
    data_key = "abandonedCheckouts"
    replication_key = "updatedAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def process_sub_entities(self, data, entity_name):
        sub_entities = []

        for item in data[entity_name]["edges"]:
            if (node := item.get("node")):
                sub_entities.append(node)

        return sub_entities

    def transform_object(self, obj):
        obj["lineItems"] = self.process_sub_entities(obj, entity_name="lineItems")
        return obj

    def get_query(self):
        qry = """
        query abandonedcheckouts($first: Int!, $after: String, $query: String) {
            abandonedCheckouts(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        note
                        completedAt
                        billingAddress {
                            phone
                            country
                            firstName
                            name
                            latitude
                            zip
                            lastName
                            province
                            address2
                            address1
                            countryCodeV2
                            city
                            company
                            provinceCode
                            longitude
                            coordinatesValidated
                            formattedArea
                            id
                            timeZone
                            validationResultSummary
                        }
                        discountCodes
                        createdAt
                        updatedAt
                        taxLines {
                            priceSet {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            title
                            rate
                            source
                            channelLiable
                            ratePercentage
                        }
                        totalLineItemsPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        id
                        name
                        totalTaxSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        shippingAddress {
                            phone
                            country
                            firstName
                            name
                            latitude
                            zip
                            lastName
                            province
                            address2
                            address1
                            countryCodeV2
                            city
                            company
                            provinceCode
                            longitude
                            coordinatesValidated
                            formattedArea
                            id
                            timeZone
                            validationResultSummary
                        }
                        abandonedCheckoutUrl
                        totalDiscountSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        taxesIncluded
                        totalDutiesSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        lineItems(first: 250) {
                            edges {
                                node {
                                    id
                                    quantity
                                    sku
                                    title
                                    variantTitle
                                    variant {
                                        title
                                        id
                                    }
                                    discountedTotalPriceSet {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    components {
                                        id
                                        quantity
                                        title
                                        variantTitle
                                    }
                                    customAttributes {
                                        key
                                        value
                                    }
                                    product {
                                        id
                                    }
                                    discountedUnitPriceSet {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    discountedUnitPriceWithCodeDiscount {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    originalTotalPriceSet {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    originalUnitPriceSet {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                        subtotalPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        return qry


Context.stream_objects["abandoned_checkouts"] = AbandonedCheckouts
