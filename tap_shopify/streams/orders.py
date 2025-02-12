import os
import sys
import json
import time

import singer
import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

LOGGER = singer.get_logger()

class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def unwrap_nodes(obj):
    """
    Recursively unwraps `nodes` keys in a nested dictionary or list.
    """
    if isinstance(obj, dict):
        # Check if the dictionary has a single key 'nodes'
        if len(obj) == 1 and "nodes" in obj:
            return unwrap_nodes(obj["nodes"])
        else:
            # Recursively process each value in the dictionary
            return {k: unwrap_nodes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        # Recursively process each item in the list
        return [unwrap_nodes(item) for item in obj]
    else:
        # If it's neither a dict nor a list, return the object as-is
        return obj

class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    gql_query = """
    query Orders($query: String, $cursor: String) {
        orders(first: 100, query: $query, after: $cursor, sortKey: UPDATED_AT) {
            nodes {
                id
                updatedAt
                presentmentCurrencyCode
                subtotalPriceSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                totalDiscountsSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                totalPriceSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                totalShippingPriceSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                totalTaxSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                test
                app {
                    id
                }
                billingAddress {
                    address1
                    address2
                    city
                    company
                    coordinatesValidated
                    country
                    countryCode
                    countryCodeV2
                    firstName
                    formatted
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
                    zip
                }
                cancelReason
                cancelledAt
                clientIp
                closedAt
                confirmed
                createdAt
                currencyCode
                customAttributes {
                    key
                    value
                }
                customer {
                    canDelete
                    createdAt
                    displayName
                    email
                    firstName
                    hasTimelineComment
                    id
                    lastName
                    legacyResourceId
                    lifetimeDuration
                    locale
                    multipassIdentifier
                    note
                    numberOfOrders
                    phone
                    productSubscriberStatus
                    state
                    tags
                    taxExempt
                    taxExemptions
                    unsubscribeUrl
                    updatedAt
                    validEmailAddress
                    verifiedEmail
                }
                customerAcceptsMarketing
                customerLocale
                discountCodes
                displayFinancialStatus
                displayFulfillmentStatus
                email
                landingPageUrl
                name
                note
                paymentGatewayNames
                phone
                processedAt
                referrerUrl
                registeredSourceUrl
                shippingAddress {
                    address1
                    address2
                    city
                    company
                    coordinatesValidated
                    country
                    countryCode
                    countryCodeV2
                    firstName
                    formatted
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
                    zip
                }
                sourceIdentifier
                tags
                totalWeight
                totalTipReceivedSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                }
                taxLines {
                    channelLiable
                    price
                    rate
                    title
                }
                refunds(first: 50) {
                    createdAt
                    id
                    legacyResourceId
                    note
                    updatedAt
                    refundLineItems(first: 50) {
                        nodes {
                            price
                            quantity
                            restockType
                            restocked
                            subtotal
                            totalTax
                            lineItem {
                                canRestock
                                currentQuantity
                                discountedTotal
                                discountedUnitPrice
                                fulfillableQuantity
                                fulfillmentStatus
                                id
                                merchantEditable
                                name
                                nonFulfillableQuantity
                                originalTotal
                                originalUnitPrice
                                quantity
                                refundableQuantity
                                requiresShipping
                                restockable
                                sku
                                taxable
                                title
                                totalDiscount
                                unfulfilledDiscountedTotal
                                unfulfilledOriginalTotal
                                unfulfilledQuantity
                                variantTitle
                                vendor
                            }
                        }
                    }
                }
                discountApplications(first: 50) {
                    nodes {
                        allocationMethod
                        index
                        targetSelection
                        targetType
                        value {
                            ... on MoneyV2 {
                                amount
                                currencyCode
                            }
                        }
                    }
                }
                fulfillments {
                    createdAt
                    deliveredAt
                    displayStatus
                    estimatedDeliveryAt
                    id
                    inTransitAt
                    legacyResourceId
                    name
                    requiresShipping
                    status
                    totalQuantity
                    updatedAt
                }
                shippingLines(first: 50) {
                    nodes {
                        carrierIdentifier
                        code
                        custom
                        deliveryCategory
                        id
                        phone
                        price
                        shippingRateHandle
                        source
                        title
                    }
                }
                lineItems(first: 50) {
                    nodes {
                        canRestock
                        currentQuantity
                        discountedTotal
                        discountedUnitPrice
                        fulfillableQuantity
                        fulfillmentStatus
                        id
                        merchantEditable
                        name
                        nonFulfillableQuantity
                        originalTotal
                        originalUnitPrice
                        quantity
                        refundableQuantity
                        requiresShipping
                        restockable
                        sku
                        taxable
                        title
                        totalDiscount
                        unfulfilledDiscountedTotal
                        unfulfilledOriginalTotal
                        unfulfilledQuantity
                        variantTitle
                        vendor
                    }
                }
            }
            pageInfo {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
            }
        }
    }
    """

    @shopify_error_handling
    def call_api_for_orders(self, gql_client, query, cursor=None):
        with HiddenPrints():
            params = dict(query=query, cursor=cursor)
            LOGGER.info(f"Making request = {self.gql_query}, {params}")
            response = gql_client.execute(self.gql_query, params)
        result = json.loads(response)
        LOGGER.info(result)

        if result.get("errors"):
            raise Exception(result['errors'])
        return result

    def get_orders(self, query):
        gql_client = shopify.GraphQL()
        page = self.call_api_for_orders(gql_client, query)
        yield page

        # paginate
        page_info = page['data']['orders']['pageInfo']
        while page_info['hasNextPage']:
            page = self.call_api_for_orders(gql_client, query, cursor=page_info['endCursor'])
            page_info = page['data']['orders']['pageInfo']
            yield page
        
    def get_objects(self):
        # get bookmark
        updated_at = self.get_bookmark().strftime('%Y-%m-%dT%H:%M:%S')
        query = f"updated_at:>'{updated_at}'"
        orders = 0
        start = time.time()

        for page in self.get_orders(query):
            for order in page['data']['orders']['nodes']:
                # TODO: need to map back to Shopify REST formatting
                orders += 1
                now = time.time()
                LOGGER.info(f"Got {orders} in {now - start} sec.")

                # unwrap nodes
                order = unwrap_nodes(order)

                yield order

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj

Context.stream_objects['orders'] = Orders
