from datetime import timedelta
import json
import time
import re
import backoff
import requests
import shopify
import singer
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
from tap_shopify.exceptions import ShopifyAPIError, BulkOperationInProgressError

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
                customAttributes {
                    key
                    value
                }
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
                number
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
                location {
                    id
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
                    lineItemGroup {
                        customAttributes {
                        key
                        value
                        }
                        id
                        quantity
                        title
                        variantId
                        variantSku
                    }
                    }
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
                retailLocation {
                activatable
                address {
                    address1
                    address2
                    city
                    country
                    countryCode
                    formatted
                    latitude
                    longitude
                    phone
                    province
                    provinceCode
                    zip
                }
                addressVerified
                createdAt
                deactivatable
                deactivatedAt
                deletable
                fulfillmentService {
                    id
                }
                fulfillsOnlineOrders
                hasActiveInventory
                hasUnfulfilledOrders
                id
                isActive
                isFulfillmentService
                legacyResourceId
                localPickupSettingsV2 {
                    instructions
                    pickupTime
                }
                name
                shipsInventory
                updatedAt
                suggestedAddresses {
                    address1
                    address2
                    city
                    country
                    countryCode
                    formatted
                    province
                    provinceCode
                    zip
                }
                }
                discountApplications {
                edges {
                    node {
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
                        title
                    }
                    ... on DiscountCodeApplication {
                        __typename
                        code
                    }
                    ... on ManualDiscountApplication {
                        title
                        description
                    }
                    ... on ScriptDiscountApplication {
                        __typename
                        title
                    }
                    }
                }
                }
            }
            }
        }
        }
        """

    def update_bookmark(self, bookmark_value, bookmark_key=None, bulk_op_metadata=None):
        # Standard Singer bookmark
        singer.write_bookmark(
            Context.state,
            self.name,
            bookmark_key or self.replication_key,
            bookmark_value
        )

        # Store under orders -> bulk_operation
        if bulk_op_metadata:
            orders_bookmark = Context.state.setdefault("bookmarks", {}).setdefault("orders", {})
            orders_bookmark["bulk_operation"] = bulk_op_metadata

        singer.write_state(Context.state)

    def build_query_filter(self, updated_at_min, updated_at_max):
        return f"updated_at:>='{updated_at_min}' AND updated_at:<'{updated_at_max}'"

    def submit_bulk_query(self, query_string):
        url = f"https://{Context.config.get('shop')}.myshopify.com/admin/api/2025-07/graphql.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": Context.config.get("api_key"),
        }
        operation = {
            "query": """
                mutation bulkOperationRunQuery($query: String!) {
                bulkOperationRunQuery(query: $query) {
                    bulkOperation {
                    id
                    status
                    createdAt
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
        response = requests.post(url, headers=headers, json=operation, timeout=300)
        LOGGER.info("X-request-ID for the bulk operation: %s", response.headers.get("X-Request-ID"))

        return response.json()

    def poll_bulk_completion(self, current_bookmark, bulk_op_id, timeout=82800):
        def fetch_bulk_operation(op_id):
            query = f"""
            {{
                node(id: "{op_id}") {{
                    ... on BulkOperation {{
                        id
                        status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                    }}
                }}
            }}
            """
            response = json.loads(shopify.GraphQL().execute(query=query))
            if not isinstance(response, dict):
                raise ShopifyAPIError(f"Unexpected GraphQL response: {response}")
            return response.get("data", {}).get("node")

        start = time.time()
        last_status = None

        while time.time() - start < timeout:
            op = fetch_bulk_operation(bulk_op_id)

            if not op:
                LOGGER.warning("Bulk operation not found: %s", bulk_op_id)
                return None
            if not isinstance(op, dict):
                raise ShopifyAPIError(f"Unexpected bulk operation format: {op}")

            current_status = op.get("status")

            if current_status != last_status:
                LOGGER.info(
                    "Bulk operation - %s, status: %s, created at - %s, completed at - %s",
                    op.get("id"),
                    current_status,
                    op.get("createdAt"),
                    op.get("completedAt") or "N/A"
                )
                last_status = current_status

            if current_status == "COMPLETED":
                LOGGER.info("Bulk operation completed. File size: %s bytes", op.get("fileSize"))
                self.update_bookmark(
                    bookmark_value=utils.strftime(current_bookmark),
                    bulk_op_metadata={
                        "bulk_operation_id": op.get("id"),
                        "status": current_status,
                        "created_at": op.get("createdAt"),
                        "last_date_window": self.date_window_size,
                    }
                )
                return op.get("url")

            if current_status in ["FAILED", "CANCELED"]:
                self.clear_bulk_operation_state()
                raise ShopifyAPIError(f"Bulk operation failed: {op.get('errorCode')}")

            time.sleep(60)

        # Save bookmark if timeout occurs
        self.update_bookmark(
            bookmark_value=utils.strftime(current_bookmark),
            bulk_op_metadata={
                "bulk_operation_id": op.get("id"),
                "status": op.get("status"),
                "created_at": op.get("createdAt"),
                "last_date_window": self.date_window_size,
            }
        )

        elapsed = int(time.time() - start)
        raise ShopifyAPIError(
            f"Bulk operation id - {op.get('id') or 'UNKNOWN'} did not complete "
            f"within {elapsed} seconds. "
            "Please contact Shopify support with the operation ID for assistance."
        )

    # pylint: disable=unsupported-assignment-operation
    def parse_bulk_jsonl(self, url):
        """
        Streams and yields one order at a time, with its associated line items,
        without holding all orders/line_items in memory.
        """
        resp = requests.get(url, stream=True, timeout=60)
        current_order = None
        current_line_items = []

        for line in resp.iter_lines():
            if not line:
                continue
            rec = json.loads(line)
            if not isinstance(rec, dict):
                LOGGER.warning("Skipping unexpected JSONL line (not a dict): %s", rec)
                continue
            # Detect line item (child) or order (parent)
            if '__parentId' in rec:
                # It's a line item belonging to current_order
                current_line_items.append(rec)
            else:
                if current_order:
                    current_order["lineItems"] = current_line_items
                    yield current_order
                # Start tracking new parent group
                current_order = rec
                current_line_items = []
        # Yield the last parent group (if exists)
        if current_order:
            current_order["lineItems"] = current_line_items
            yield current_order

    def transform_object(self, obj):
        if obj.get("lineItems", {}).get("edges"):
            obj["lineItems"] = [item["node"] for item in obj["lineItems"]["edges"]]
        return obj

    def clear_bulk_operation_state(self):
        orders_bookmark = Context.state.get("bookmarks", {}).get("orders", {})
        if "bulk_operation" in orders_bookmark:
            del orders_bookmark["bulk_operation"]
            singer.write_state(Context.state)

    # pylint: disable=too-many-locals,too-many-statements
    def get_objects(self):
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)
        query_template = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info(
            "GraphQL query for stream '%s': %s",
            self.name,
            ' '.join(query_template.split())
        )

        bulk_op = Context.state.get("bookmarks", {}).get("orders", {}).get("bulk_operation")
        op_id = None
        existing_url = None

        if bulk_op:
            if bulk_op.get("last_date_window") != self.date_window_size:
                LOGGER.info(
                    "Clearing existing bulk operation state due to date "
                    "window size change from %s to %s",
                    bulk_op.get("last_date_window"),
                    self.date_window_size
                )
                self.clear_bulk_operation_state()

            else:
                op_id = bulk_op.get("bulk_operation_id")
                op_status = bulk_op.get("status")

                if op_status in ["RUNNING", "COMPLETED"]:
                    LOGGER.info("Resuming polling for existing bulk operation ID: %s", op_id)
                    existing_url = self.poll_bulk_completion(current_bookmark, op_id)
                else:
                    self.clear_bulk_operation_state()

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)

            if not existing_url:
                existing_url = self.submit_and_poll_bulk_query(
                    query_template,
                    last_updated_at,
                    query_end,
                    current_bookmark
                )
            if existing_url:
                for obj in self.parse_bulk_jsonl(existing_url):
                    replication_value = utils.strptime_to_utc(obj[self.replication_key])
                    current_bookmark = max(current_bookmark, replication_value)

                    yield obj
            else:
                LOGGER.info("No data returned for the date range: %s to %s",
                               last_updated_at, query_end)

            self.clear_bulk_operation_state()
            last_updated_at = query_end
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))
            existing_url = None

    @backoff.on_exception(
        backoff.expo,
        BulkOperationInProgressError,
        max_tries=7,
        factor=10,
        jitter=None,
        on_backoff=lambda details: LOGGER.warning(
            "Bulk operation already in progress (ID: %s). "
            "Retry attempt %d after %.2f seconds. Total elapsed: %.2f seconds.",
            getattr(details['exception'], 'bulk_op_id', 'UNKNOWN'),
            details['tries'],
            details['wait'],
            details['elapsed']
        )
    )
    def submit_and_poll_bulk_query(
        self, query_template, last_updated_at, query_end, current_bookmark
        ):
        """Submit bulk query and poll for completion with automatic retry on conflicts"""
        with metrics.http_request_timer(self.name):
            query_filter = self.build_query_filter(
                utils.strftime(last_updated_at),
                utils.strftime(query_end)
            )
            query = query_template % query_filter
            LOGGER.info("Fetching records in date range: %s", query_filter)

            bulk_op_data = self.submit_bulk_query(query)

            user_errors = (
                bulk_op_data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            )

            if user_errors:
                for error in user_errors:
                    message = error.get("message", "")
                    if (
                        "bulk query operation for this app and shop is already in progress"
                        in message
                        ):
                        # Extract BulkOperation ID using regex
                        match = re.search(r"gid://shopify/BulkOperation/\d+", message)
                        bulk_op_id = match.group(0) if match else None

                        LOGGER.info("Detected concurrent bulk operation (ID: %s)", bulk_op_id)
                        raise BulkOperationInProgressError(
                            f"Bulk operation already in progress: {bulk_op_id}",
                            bulk_op_id=bulk_op_id
                        )

                # Handle other user errors
                raise ShopifyAPIError("Bulk query error: {}".format(user_errors))

            bulk_operation = (
                bulk_op_data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("bulkOperation")
            )
            bulk_op_id = bulk_operation.get("id") if bulk_operation else None
            if not bulk_op_id:
                raise ShopifyAPIError("Invalid bulk operation response: {}".format(bulk_op_data))

            return self.poll_bulk_completion(current_bookmark, bulk_op_id)


Context.stream_objects["orders"] = Orders
