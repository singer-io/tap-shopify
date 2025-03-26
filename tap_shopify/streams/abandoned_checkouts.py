from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class AbandonedCheckouts(Stream):
    """Stream class for Abandoned Checkouts in Shopify."""
    name = "abandoned_checkouts"
    data_key = "abandonedCheckouts"
    replication_key = "updatedAt"

    @classmethod
    def process_sub_entities(cls, data, entity_name):
        """
        Processes sub-entities from the response data.

        Args:
            data (dict): Response data.
            entity_name (str): Name of the entity to process.

        Returns:
            list: List of processed sub-entities.
        """
        sub_entities = []
        for item in data[entity_name]["edges"]:
            if node := item.get("node"):
                sub_entities.append(node)
        return sub_entities

    def transform_object(self, obj):
        """
        Transforms the object by processing its sub-entities.

        Args:
            obj (dict): Object to transform.

        Returns:
            dict: Transformed object.
        """
        obj["lineItems"] = self.process_sub_entities(obj, entity_name="lineItems")
        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching abandoned checkouts.

        Returns:
            str: GraphQL query string.
        """
        return """
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


Context.stream_objects["abandoned_checkouts"] = AbandonedCheckouts
