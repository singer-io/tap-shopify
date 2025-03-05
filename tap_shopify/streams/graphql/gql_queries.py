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
                        inventoryItem { 
                            id 
                            measurement {
                                weight {
                                    unit
                                    value
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
