from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Orders(Stream):
    """Stream class for Shopify Orders."""

    name = "orders"
    data_key = "orders"
    replication_key = "updatedAt"

    def transform_lineitems(self, data):
        """
        Transforms the order lineitems data by extracting order IDs and handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of lineItems.
        """
        lineitems = [
            node for item in data["lineItems"]["edges"]
            if (node := item.get("node"))
        ]

        # Handle pagination
        page_info = data["lineItems"].get("pageInfo", {})
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{data['id'].split('/')[-1]}",
                "childafter": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            lineitems_data = response.get("edges", [])[0].get("node", {})
            lineitems.extend(
                node for item in lineitems_data["lineItems"]["edges"]
                if (node := item.get("node"))
            )
            page_info = lineitems_data.get("pageInfo", {})

        return lineitems

    def transform_object(self, obj):
        """
        Transforms a collection object.

        Args:
            obj (dict): Collection object.

        Returns:
            dict: Transformed collection object.
        """
        if obj.get("lineItems"):
            obj["lineItems"] = self.transform_lineitems(obj)
        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching orders.

        Returns:
            str: GraphQL query string.
        """
        return """
        query orders($first: Int!, $after: String, $query: String, $childafter: String) {
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
                            addresses {
                                address1
                                address2
                                city
                                countryCodeV2
                                country
                                company
                                firstName
                                lastName
                                id
                                name
                                phone
                                province
                                provinceCode
                                zip
                            }
                            state
                            verifiedEmail
                            updatedAt
                            taxExempt
                            tags
                            taxExemptions
                            note
                            multipassIdentifier
                            createdAt
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
                                name
                                province
                                phone
                                provinceCode
                                zip
                            }
                        }
                        customerJourneySummary {
                            lastVisit {
                                landingPage
                                referrerUrl
                            }
                        }
                        merchantOfRecordApp {
                            id
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
                        fulfillments(first: 250) {
                            id
                            name
                            status
                            totalQuantity
                            updatedAt
                            createdAt
                            deliveredAt
                            estimatedDeliveryAt
                            requiresShipping
                            inTransitAt
                            trackingInfo(first: 250) {
                                number
                                company
                                url
                            }
                            service {
                                serviceName
                                id
                                handle
                                trackingSupport
                                type
                                permitsSkuSharing
                                inventoryManagement
                            }
                        }
                        lineItems(first: 25, after: $childafter) {
                            edges {
                                node {
                                    id
                                    vendor
                                    quantity
                                    title
                                    requiresShipping
                                    originalTotalSet {
                                        presentmentMoney {
                                            currencyCode
                                            amount
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    taxLines(first: 250) {
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
                                        title
                                        source
                                        channelLiable
                                    }
                                    taxable
                                    isGiftCard
                                    name
                                    discountedTotalSet {
                                        presentmentMoney {
                                            amount
                                            currencyCode
                                        }
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    sku
                                    product {
                                        id
                                    }
                                    discountAllocations {
                                        allocatedAmountSet {
                                            presentmentMoney {
                                                amount
                                                currencyCode
                                            }
                                            shopMoney {
                                                amount
                                                currencyCode
                                            }
                                        }
                                        discountApplication {
                                            index
                                            targetType
                                            targetSelection
                                            allocationMethod
                                            value {
                                                ... on MoneyV2 {
                                                    __typename
                                                    amount
                                                    currencyCode
                                                }
                                                ... on PricingPercentageValue {
                                                    __typename
                                                    percentage
                                                }
                                            }
                                        }
                                    }
                                    customAttributes {
                                        key
                                        value
                                    }
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
                                    duties {
                                        harmonizedSystemCode
                                        id
                                        taxLines {
                                            rate
                                            source
                                            title
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
                                        }
                                        countryCodeOfOrigin
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
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
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


Context.stream_objects["orders"] = Orders
