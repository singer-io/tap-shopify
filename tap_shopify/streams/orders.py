from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Orders(Stream):
    """Stream class for Shopify Orders."""

    name = "orders"
    data_key = "orders"
    replication_key = "updatedAt"

    def get_query(self):
        """
        Returns the GraphQL query for fetching orders.

        Returns:
            str: GraphQL query string.
        """
        return """
        query orders($first: Int!, $after: String, $query: String) {
            orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                edges {
                    node {
                        additionalFees {
                            id
                            name
                            price {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            taxLines {
                                channelLiable
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
                                rate
                                ratePercentage
                                source
                                title
                            }
                        }
                        alerts {
                            content
                            dismissibleHandle
                            icon
                            severity
                            title
                            actions {
                                primary
                                show
                                title
                                url
                            }
                        }
                        app {
                            id
                            name
                            icon {
                                id
                            }
                        }
                        billingAddress {
                            address1
                            address2
                            city
                            company
                            coordinatesValidated
                            country
                            countryCodeV2
                            firstName
                            formattedArea
                            id
                            lastName
                            latitude
                            longitude
                            name
                            phone
                            province
                            provinceCode
                            timeZone
                            validationResultSummary
                            zip
                        }
                        billingAddressMatchesShippingAddress
                        canMarkAsPaid
                        canNotifyCustomer
                        cancelReason
                        cancellation {
                            staffNote
                        }
                        cancelledAt
                        capturable
                        cartDiscountAmountSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        channelInformation {
                            id
                            channelId
                        }
                        clientIp
                        closed
                        closedAt
                        confirmationNumber
                        confirmed
                        createdAt
                        currencyCode
                        currentCartDiscountAmountSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentShippingPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentSubtotalLineItemsQuantity
                        currentSubtotalPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTaxLines {
                            channelLiable
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
                            rate
                            ratePercentage
                            source
                            title
                        }
                        currentTotalAdditionalFeesSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTotalDiscountsSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTotalDutiesSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTotalPriceSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTotalTaxSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        currentTotalWeight
                        customer {
                            id
                            email
                            firstName
                            lastName
                        }
                        customerAcceptsMarketing
                        customerLocale
                        discountCodes
                        discountCode
                        displayFinancialStatus
                        displayFulfillmentStatus
                        disputes {
                            id
                            initiatedAs
                            status
                        }
                        dutiesIncluded
                        email
                        edited
                        estimatedTaxes
                        fulfillable
                        fullyPaid
                        hasTimelineComment
                        fulfillmentsCount {
                            count
                            precision
                        }
                        id
                        legacyResourceId
                        merchantBusinessEntity {
                            address {
                                address1
                                address2
                                city
                                countryCode
                                province
                                zip
                            }
                            companyName
                            displayName
                            id
                            primary
                        }
                        name
                        note
                        netPaymentSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        originalTotalAdditionalFeesSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        originalTotalDutiesSet {
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
                        paymentGatewayNames
                        phone
                        poNumber
                        presentmentCurrencyCode
                        processedAt
                        refundable
                        refundDiscrepancySet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        registeredSourceUrl
                        requiresShipping
                        restockable
                        returnStatus
                        shippingAddress {
                            address1
                            address2
                            city
                            company
                            coordinatesValidated
                            country
                            countryCodeV2
                            firstName
                            formattedArea
                            id
                            lastName
                            latitude
                            longitude
                            name
                            phone
                            province
                            provinceCode
                            timeZone
                            validationResultSummary
                            zip
                        }
                        shopifyProtect {
                            eligibility {
                                status
                            }
                            status
                        }
                        sourceIdentifier
                        sourceName
                        statusPageUrl
                        subtotalLineItemsQuantity
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
                        tags
                        taxExempt
                        taxLines {
                            channelLiable
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
                            rate
                            ratePercentage
                            source
                            title
                        }
                        taxesIncluded
                        test
                        totalCapturableSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalCashRoundingAdjustment {
                            paymentSet {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            refundSet {
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
                        totalDiscountsSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalOutstandingSet {
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
                        totalReceivedSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalRefundedSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalRefundedShippingSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
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
                        totalTipReceivedSet {
                            presentmentMoney {
                                amount
                                currencyCode
                            }
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        totalWeight
                        transactionsCount {
                            count
                            precision
                        }
                        unpaid
                        updatedAt
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """


Context.stream_objects["orders"] = Orders
