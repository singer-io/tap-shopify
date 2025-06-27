from datetime import timedelta
import json
import time
import requests
import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
import singer
from singer import metrics, utils
from tap_shopify.exceptions import ShopifyError

LOGGER = singer.get_logger()
class Orders(Stream):
    name = "orders"
    data_key = "orders"
    replication_key = "updatedAt"

    def get_query(self):
        """
        Returns the GraphQL query string for the bulk operation.
        The date filters will be injected via the bulk operation variables.
        """
        return """
        {
          orders(query: "%s") {
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
                    fulfillments {
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
                    trackingInfo {
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
                    lineItems {
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
                            ... on AutomaticDiscountApplication {
                                title
                            }
                            ... on ManualDiscountApplication {
                                title
                            }
                            ... on ScriptDiscountApplication {
                                title
                            }
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
                        unfulfilledDiscountedTotalSet {
                            presentmentMoney {
                            amount
                            currencyCode
                            }
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        unfulfilledOriginalTotalSet {
                            presentmentMoney {
                            amount
                            currencyCode
                            }
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        variant {
                            id
                        }
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    }
                    shippingLine {
                    carrierIdentifier
                    code
                    currentDiscountedPriceSet {
                        presentmentMoney {
                        amount
                        currencyCode
                        }
                        shopMoney {
                        amount
                        currencyCode
                        }
                    }
                    custom
                    deliveryCategory
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
                        allocationMethod
                        index
                        targetSelection
                        targetType
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
                        ... on AutomaticDiscountApplication {
                            __typename
                            allocationMethod
                            index
                            targetSelection
                            targetType
                            title
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
                        ... on DiscountCodeApplication {
                            __typename
                            allocationMethod
                            code
                            index
                            targetSelection
                            targetType
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
                        ... on ManualDiscountApplication {
                            description
                            allocationMethod
                            index
                            targetSelection
                            targetType
                            title
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
                        ... on ScriptDiscountApplication {
                            __typename
                            allocationMethod
                            index
                            targetSelection
                            targetType
                            title
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
                    }
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
                    id
                    isRemoved
                    originalPriceSet {
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
                    shippingRateHandle
                    source
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
                    title
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

    def build_query_filter(self, updated_at_min, updated_at_max):
        return f"updated_at:>='{updated_at_min}' AND updated_at:<'{updated_at_max}'"

    def submit_bulk_query(self, query_string):
        operation = {
            "query": """
                mutation bulkOperationRunQuery($query: String!) {
                  bulkOperationRunQuery(query: $query) {
                    bulkOperation {
                      id
                      status
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
            """,
            "variables": {
                "query": query_string
            }
        }
        return shopify.GraphQL().execute(**operation)

    def poll_bulk_completion(self, timeout=7200):
        start = time.time()
        wait = 60
        while time.time() - start < timeout:
            response = json.loads(shopify.GraphQL().execute(query="""
                {
                currentBulkOperation {
                    id
                    status
                    errorCode
                    createdAt
                    completedAt
                    objectCount
                    fileSize
                    url
                }
                }
            """))

            if not isinstance(response, dict):
                raise ShopifyError(f"Unexpected GraphQL response: {response}")

            op = response.get("data", {}).get("currentBulkOperation")

            if not isinstance(op, dict):
                raise ShopifyError(f"Unexpected bulk operation format: {op}")

            if op.get("status") == "COMPLETED":
                return op.get("url")
            elif op.get("status") in ["FAILED", "CANCELED"]:
                raise Exception(f"Bulk operation failed: {op.get('errorCode')}")

            time.sleep(wait)

        raise Exception("Timed out waiting for bulk operation.")

    def parse_bulk_jsonl(self, url):
        orders, line_items = {}, {}
        resp = requests.get(url, stream=True)
        for line in resp.iter_lines():
            if line:
                rec = json.loads(line)
                if "__parentId" in rec:
                    line_items.setdefault(rec["__parentId"], []).append(rec)
                else:
                    orders[rec["id"]] = rec

        for oid, order in orders.items():
            order["lineItems"] = line_items.get(oid, [])
        return list(orders.values())

    def transform_object(self, obj):
        if obj.get("lineItems", {}).get("edges"):
            obj["lineItems"] = [item["node"] for item in obj["lineItems"]["edges"]]
        return obj

    def get_objects(self):
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)
        query = self.get_query()
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)

            with metrics.http_request_timer(self.name):
                query_filter = self.build_query_filter(
                    utils.strftime(last_updated_at),
                    utils.strftime(query_end)
                )
                query = self.get_query() % query_filter
                LOGGER.info("Fetching the records in the date range of %s", query_filter)
                self.submit_bulk_query(query)
                url = self.poll_bulk_completion()

            if url:
              for obj in self.parse_bulk_jsonl(url):
                  # obj = self.transform_object(obj)
                  replication_value = utils.strptime_to_utc(obj[self.replication_key])
                  current_bookmark = max(current_bookmark, replication_value)
                  yield obj
            else:
                LOGGER.warning("No data returned for the date range: %s to %s", last_updated_at, query_end)

            last_updated_at = query_end
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

Context.stream_objects["orders"] = Orders
