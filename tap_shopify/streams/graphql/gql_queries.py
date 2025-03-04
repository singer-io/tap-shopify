"""
Stores all the GraphQl Queries for shopify api
"""

def get_products_query():
    """
    Returns GraphQL query to get all products
    """
    return """
        query GetProducts($first: Int!, $after: String, $query: String) {
            products(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        title
                        descriptionHtml
                        vendor
                        category {
                            id
                        }
                        tags
                        handle
                        publishedAt
                        createdAt
                        updatedAt
                        templateSuffix
                        status
                        productType
                        options {
                            id
                            name
                            position
                            values
                        }
                        giftCardTemplateSuffix
                        hasOnlyDefaultVariant
                        hasOutOfStockVariants
                        hasVariantsThatRequiresComponents
                        isGiftCard
                        description
                        compareAtPriceRange {
                            maxVariantCompareAtPrice {
                                amount
                                currencyCode
                            }
                            minVariantCompareAtPrice {
                                amount
                                currencyCode
                            }
                        }
                        featuredMedia {
                            id
                            mediaContentType
                            status
                        }
                        requiresSellingPlan
                        totalInventory
                        tracksInventory
                        media(first: 250) {
                            edges {
                                node {
                                    id
                                    alt
                                    status
                                    mediaContentType
                                    mediaWarnings {
                                        code
                                        message
                                    }
                                    mediaErrors {
                                        code
                                        details
                                        message
                                    }
                                    ... on ExternalVideo {
                                        id
                                        embedUrl
                                    }
                                    ... on MediaImage {
                                        id
                                        updatedAt
                                        createdAt
                                        mimeType
                                        image {
                                            url
                                            width
                                            height
                                            id
                                        }
                                    }
                                    ... on Model3d {
                                        id
                                        filename
                                        sources {
                                            url
                                            format
                                            mimeType
                                            filesize
                                        }
                                    }
                                    ... on Video {
                                        id
                                        updatedAt
                                        createdAt
                                        filename
                                        sources {
                                            url
                                            format
                                            mimeType
                                            fileSize
                                        }

                                    }
                                }
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

def get_product_variant_query():
    """
    Returns GraphQL query to get all product variants
    """
    return """
        query GetProductVariants($first: Int!, $after: String, $query: String) {
            productVariants(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        createdAt
                        barcode
                        availableForSale
                        compareAtPrice
                        displayName
                        image {
                            altText
                            height
                            id
                            url
                            width
                        }
                        inventoryPolicy
                        inventoryQuantity
                        position
                        price
                        requiresComponents
                        sellableOnlineQuantity
                        sku
                        taxCode
                        taxable
                        title
                        updatedAt
                        product { id }
                        inventoryItem { id }
                    }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
            }
        }
        """

def get_inventory_items_query():
    """
    Returns GraphQL query to get all inventory items
    """
    return """
            query GetinventoryItems($first: Int!, $after: String, $query: String) {
                inventoryItems(first: $first, after: $after, query: $query) {
                    edges {
                        node {
                            id
                            createdAt
                            sku
                            updatedAt
                            requiresShipping
                            countryCodeOfOrigin
                            provinceCodeOfOrigin
                            harmonizedSystemCode
                            tracked
                            unitCost {
                                amount
                            }
                            countryHarmonizedSystemCodes(first: 175) {
                                edges {
                                    node {
                                        countryCode
                                        harmonizedSystemCode
                                    }
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

def get_parent_ids_query(resource):
    """
    Returns GraphQL query to get id for all metaftield
    supported parent streams.
    """

    qry = """
    query getParentEntities( $first: Int!, $after: String $query: String) {
        RESOURCE(first: $first after: $after query: $query) {
            edges {
                node {
                    id
                }
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
    """
    qry = qry.replace("RESOURCE", resource)
    return qry

def get_metafields_query(resource):
    """Returns the GraphQL query for fetching metafields"""
    if resource == 'shop':
        return get_metafield_query_shop()

    qry = """
        query getMetafields( $first: Int!, $after: String $query: String) {
        RESOURCE(first: $first after: $after query: $query) {
            edges {
            node {
                metafields(first: $first) {
                edges {
                    node {
                        id
                        ownerType
                        value
                        type
                        key
                        createdAt
                        namespace
                        description
                        updatedAt
                        owner {
                            ... on Customer {
                            id
                            }
                            ... on Product {
                            id
                            }
                            ... on Order {
                            id
                            }
                            ... on Collection {
                            id
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

    qry = qry.replace("RESOURCE", resource)
    return qry

def get_metafield_query_customers():
    """
    Returns GraphQL query to get customer metafields
    """
    qry = """
        query GetMetafields($pk_id: ID! $first: Int!, $after: String) {
            customer(id: $pk_id) {
                metafields(first: $first after: $after ){
                    edges {
                        node {
                            id
                            ownerType
                            value
                            type
                            key
                            createdAt
                            namespace
                            description
                            updatedAt
                            owner {
                                ... on Customer {
                                    id
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
    """
    return qry

def get_metafield_query_product():
    """
    Returns GraphQL query to get product metafields
    """
    qry = """
        query GetMetafields($pk_id: ID! $first: Int!, $after: String) {
            product(id: $pk_id) {
                metafields(first: $first after: $after ){
                    edges {
                        node {
                            id
                            ownerType
                            value
                            type
                            key
                            createdAt
                            namespace
                            description
                            updatedAt
                            owner {
                                ... on Product {
                                    id
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
    """
    return qry

def get_metafield_query_collection():
    """
    Returns GraphQL query to get collection metafields
    """
    qry = """
        query GetMetafields($pk_id: ID! $first: Int!, $after: String) {
            collection(id: $pk_id) {
                metafields(first: $first after: $after ){
                    edges {
                        node {
                            id
                            ownerType
                            value
                            type
                            key
                            createdAt
                            namespace
                            description
                            updatedAt
                            owner {
                                ... on Collection {
                                    id
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
    """
    return qry

def get_metafield_query_order():
    """
    Returns GraphQL query to get order metafields
    """
    qry = """
        query GetMetafields($pk_id: ID! $first: Int!, $after: String) {
            order(id: $pk_id) {
                metafields(first: $first after: $after ){
                    edges {
                        node {
                            id
                            ownerType
                            value
                            type
                            key
                            createdAt
                            namespace
                            description
                            updatedAt
                            owner {
                                ... on Order {
                                    id
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
    """
    return qry

def get_metafield_query_shop():
    """
    Returns GraphQL query to get shop metafields
    """
    qry = """
        query GetMetafields($first: Int!, $after: String) {
            shop{
                metafields(first: $first after: $after ){
                    edges {
                        node {
                            id
                            ownerType
                            value
                            type
                            key
                            createdAt
                            namespace
                            description
                            updatedAt
                            owner {
                                ... on Shop {
                                    id
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
    """
    return qry

def get_abandoned_checkouts_query():
    qry = """query abandonedcheckouts {
                abandonedCheckouts(first: 10) {
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
                        lineItems(first: 10, after: "") {
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
                            }
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
                    }
                    }
                    pageInfo {
                    hasNextPage
                    endCursor
                    }
                }
                }"""

def get_collections_query():
    qry = """
        query MyQuery {
            collections(first: 10) {
                edges {
                node {
                    id
                    title
                    handle
                    description
                    updatedAt
                    productsCount {
                    count
                    }
                    sortOrder
                }
                }
            }
            }
            """
    return qry

def get_custom_collections_query():
    qry = """
            query MyQuery {
            collections(first: 10, query: "collection_type:custom") {
                edges {
                node {
                    id
                    title
                    handle
                    description
                    updatedAt
                    productsCount {
                    count
                    }
                    sortOrder
                }
                }
            }
            }"""

def get_customers_query():
    qry = """
            query Customers($first: Int!, $after: String, $query: String) {
                customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                    node {
                        email
                        multipassIdentifier
                        defaultAddress {
                        city
                        address1
                        zip
                        id
                        province
                        phone
                        country
                        firstName
                        lastName
                        countryCodeV2
                        name
                        provinceCode
                        address2
                        company
                        timeZone
                        validationResultSummary
                        latitude
                        longitude
                        coordinatesValidated
                        formattedArea
                        }
                        numberOfOrders
                        state
                        verifiedEmail
                        firstName
                        updatedAt
                        note
                        phone
                        addresses(first: 250) {
                        city
                        address1
                        zip
                        id
                        province
                        phone
                        country
                        firstName
                        lastName
                        countryCodeV2
                        name
                        provinceCode
                        address2
                        company
                        timeZone
                        validationResultSummary
                        latitude
                        longitude
                        coordinatesValidated
                        formattedArea
                        }
                        lastName
                        tags
                        taxExempt
                        id
                        createdAt
                        taxExemptions
                        emailMarketingConsent {
                        consentUpdatedAt
                        marketingOptInLevel
                        marketingState
                        }
                        smsMarketingConsent {
                        consentCollectedFrom
                        consentUpdatedAt
                        marketingOptInLevel
                        marketingState
                        }
                        validEmailAddress
                        productSubscriberStatus
                        amountSpent {
                        amount
                        currencyCode
                        }
                        dataSaleOptOut
                        displayName
                        locale
                        lifetimeDuration
                    }
                    }
                    pageInfo {
                    endCursor
                    hasNextPage
                    }
                }
                }"""
    return qry

def get_events_query():
    qry = """
        query GetEvents($first: Int!, $after: String, $query: String) {
            events(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
                edges {
                node {
                    id
                    createdAt
                    action
                    appTitle
                    attributeToApp
                    attributeToUser
                    criticalAlert
                    message
                    ... on BasicEvent {
                    id
                    subjectId
                    subjectType
                    action
                    additionalContent
                    additionalData
                    appTitle
                    arguments
                    attributeToApp
                    attributeToUser
                    createdAt
                    criticalAlert
                    hasAdditionalContent
                    message
                    secondaryMessage
                    }
                    ... on CommentEvent {
                    id
                    action
                    appTitle
                    attachments {
                        fileExtension
                        id
                        name
                        size
                        url
                    }
                    attributeToApp
                    attributeToUser
                    author {
                        id
                    }
                    canDelete
                    canEdit
                    createdAt
                    criticalAlert
                    edited
                    message
                    rawMessage
                    }
                }
                }
                pageInfo {
                endCursor
                hasNextPage
                }
            }
        }"""
    return qry

def get_inventory_levels_query():
    qry = """query GetInventoryLevels($first: Int!, $after: String, $query: String) {
                locations(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
                    edges {
                    node {
                        inventoryLevels(first: 10) {
                        edges {
                            node {
                            canDeactivate
                            createdAt
                            deactivationAlert
                            id
                            item {
                                id
                            }
                            location {
                                id
                            }
                            updatedAt
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
                }"""
    return qry

def get_locations_query():
    qry = """query GetLocations($first: Int!, $after: String, $query: String) {
                locations(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                    node {
                        address {
                        countryCode
                        address1
                        city
                        address2
                        provinceCode
                        zip
                        province
                        phone
                        country
                        formatted
                        latitude
                        longitude
                        }
                        name
                        id
                        updatedAt
                        createdAt
                        isActive
                        addressVerified
                        deactivatable
                        deactivatedAt
                        deletable
                        fulfillsOnlineOrders
                        hasActiveInventory
                        hasUnfulfilledOrders
                        isFulfillmentService
                        legacyResourceId
                        localPickupSettingsV2 {
                        instructions
                        pickupTime
                        }
                        shipsInventory
                    }
                    }
                    pageInfo {
                    endCursor
                    hasNextPage
                    }
                }
                }"""
    return qry

def get_order_refunds_query():
    qry = """
            query order_refunds {
                orders(first: 10) {
                    edges {
                    node {
                        refunds(first: 10) {
                        id
                        refundLineItems(first: 10) {
                            edges {
                            node {
                                id
                                quantity
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
                                restockType
                                restocked
                                subtotalSet {
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
                            }
                            }
                            pageInfo {
                            endCursor
                            hasNextPage
                            }
                        }
                        createdAt
                        legacyResourceId
                        note
                        order {
                            id
                        }
                        }
                    }
                    }
                    pageInfo {
                    endCursor
                    hasNextPage
                    }
                }
            }"""
    return qry

def get_orders_query():
    qry = """
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
        }"""
    return qry

def get_transactions_query():
    qry = """
        query transactions {
            orders(first: 10) {
                edges {
                node {
                    transactions(first: 10) {
                    accountNumber
                    amountRoundingSet {
                        presentmentMoney {
                        amount
                        currencyCode
                        }
                        shopMoney {
                        amount
                        currencyCode
                        }
                    }
                    amountSet {
                        presentmentMoney {
                        amount
                        currencyCode
                        }
                        shopMoney {
                        amount
                        currencyCode
                        }
                    }
                    authorizationCode
                    authorizationExpiresAt
                    createdAt
                    errorCode
                    fees {
                        amount {
                        amount
                        currencyCode
                        }
                        flatFee {
                        amount
                        currencyCode
                        }
                        flatFeeName
                        id
                        rate
                        rateName
                        taxAmount {
                        amount
                        currencyCode
                        }
                        type
                    }
                    formattedGateway
                    gateway
                    id
                    kind
                    manualPaymentGateway
                    maximumRefundableV2 {
                        amount
                        currencyCode
                    }
                    multiCapturable
                    parentTransaction {
                        accountNumber
                        createdAt
                        id
                        status
                        paymentId
                        processedAt
                        amountSet {
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
                    paymentId
                    processedAt
                    receiptJson
                    settlementCurrency
                    settlementCurrencyRate
                    shopifyPaymentsSet {
                        extendedAuthorizationSet {
                        extendedAuthorizationExpiresAt
                        standardAuthorizationExpiresAt
                        }
                        refundSet {
                        acquirerReferenceNumber
                        }
                    }
                    status
                    test
                    totalUnsettledSet {
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
                }
            }
            }"""
