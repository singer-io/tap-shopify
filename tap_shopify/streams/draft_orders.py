from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class DraftOrders(Stream):
    """Stream class for Draft Orders in Shopify."""
    name = "draft_orders"
    data_key = "draftOrders"
    replication_key = "updatedAt"
    access_scope = ["read_draft_orders"]

    def get_query(self):
        return """
        query DraftOrders($first: Int!, $after: String, $query: String) {
            draftOrders(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        note2
                        email
                        taxesIncluded
                        currencyCode
                        invoiceSentAt
                        createdAt
                        updatedAt
                        taxExempt
                        completedAt
                        name
                        status
                        invoiceUrl
                        tags
                        phone
                        totalShippingPriceSet {
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
                        lineItems(first: 250) {
                        edges {
                            node {
                                id
                                name
                                quantity
                                sku
                                title
                                requiresShipping
                                taxable
                                custom
                                variant {
                                    id
                                    title
                                }
                                weight {
                                    value
                                    unit
                                }
                                vendor
                                taxLines {
                                    title
                                    source
                                    rate
                                    ratePercentage
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
                                }
                                product {
                                    id
                                    title
                                }
                                isGiftCard
                                originalUnitPriceWithCurrency {
                                    amount
                                    currencyCode
                                }
                                }
                            }
                        }
                        shippingAddress {
                            address1
                            address2
                            city
                            company
                            country
                            countryCodeV2
                            firstName
                            latitude
                            longitude
                            lastName
                            name
                            phone
                            provinceCode
                            province
                            zip
                        }
                        shippingLine {
                            id
                            title
                            carrierIdentifier
                            custom
                            code
                            deliveryCategory
                            source
                            discountedPriceSet {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            phone
                        }
                        taxLines {
                            channelLiable
                            rate
                            ratePercentage
                            source
                            title
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
                        }
                        customer {
                            id
                            createdAt
                            lastName
                            firstName
                            defaultAddress {
                                address1
                                address2
                                city
                                company
                                country
                                countryCodeV2
                                firstName
                                id
                                lastName
                                longitude
                                name
                                phone
                                province
                                provinceCode
                                zip
                            }
                            taxExempt
                            tags
                            defaultEmailAddress {
                                emailAddress
                            }
                            lastOrder {
                                id
                                name
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
        }
        """

Context.stream_objects["draft_orders"] = DraftOrders
