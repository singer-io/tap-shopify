"""
Stores all the GraphQl Queries for shopify api
"""

def get_products_query():
    """
    product stream get query
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
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        """

def get_parent_ids(resource):
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
                startCursor
            }
        }
    }
    """
    qry = qry.replace("RESOURCE", resource)
    return qry

def get_metafield_query_customers():
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
                                ... on Validation {
                                    id
                                }
                                ... on Shop {
                                    id
                                    email
                                }
                                ... on ProductVariant {
                                    id
                                }
                                ... on Product {
                                    id
                                }
                                ... on PaymentCustomization {
                                    id
                                }
                                ... on Order {
                                    id
                                    email
                                }
                                ... on MediaImage {
                                    id
                                }
                                ... on Market {
                                    id
                                    name
                                }
                                ... on Location {
                                    id
                                    name
                                }
                                ... on Image {
                                    id
                                }
                                ... on FulfillmentConstraintRule {
                                    id
                                }
                                ... on AppInstallation {
                                    id
                                }
                                ... on CartTransform {
                                    id
                                }
                                ... on DraftOrder {
                                    id
                                }
                                ... on DiscountNode {
                                    id
                                }
                                ... on DiscountCodeNode {
                                    id
                                }
                                ... on DiscountAutomaticNode {
                                    id
                                }
                                ... on DeliveryCustomization {
                                    id
                                }
                                ... on CustomerSegmentMember {
                                    id
                                }
                                ... on Customer {
                                    id
                                }
                                ... on CompanyLocation {
                                    id
                                }
                                ... on Company {
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
    """
    return qry

def get_metafield_query_product():
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
                                ... on Validation {
                                    id
                                }
                                ... on Shop {
                                    id
                                    email
                                }
                                ... on ProductVariant {
                                    id
                                }
                                ... on Product {
                                    id
                                }
                                ... on PaymentCustomization {
                                    id
                                }

                                ... on Order {
                                    id
                                    email
                                }
                                ... on MediaImage {
                                    id
                                }
                                ... on Market {
                                    id
                                    name
                                }
                                ... on Location {
                                    id
                                    name
                                }
                                ... on Image {
                                    id
                                }
                                ... on FulfillmentConstraintRule {
                                    id
                                }
                                ... on AppInstallation {
                                    id
                                }
                                ... on CartTransform {
                                    id
                                }
                                ... on DraftOrder {
                                    id
                                }
                                ... on DiscountNode {
                                    id
                                }
                                ... on DiscountCodeNode {
                                    id
                                }
                                ... on DiscountAutomaticNode {
                                    id
                                }
                                ... on DeliveryCustomization {
                                    id
                                }
                                ... on CustomerSegmentMember {
                                    id
                                }
                                ... on Customer {
                                    id
                                }
                                ... on CompanyLocation {
                                    id
                                }
                                ... on Company {
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
    """
    return qry

def get_metafield_query_collection():
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
                                ... on Validation {
                                    id
                                }
                                ... on Shop {
                                    id
                                    email
                                }
                                ... on ProductVariant {
                                    id
                                }
                                ... on Product {
                                    id
                                }
                                ... on PaymentCustomization {
                                    id
                                }

                                ... on Order {
                                    id
                                    email
                                }
                                ... on MediaImage {
                                    id
                                }
                                ... on Market {
                                    id
                                    name
                                }
                                ... on Location {
                                    id
                                    name
                                }
                                ... on Image {
                                    id
                                }
                                ... on FulfillmentConstraintRule {
                                    id
                                }
                                ... on AppInstallation {
                                    id
                                }
                                ... on CartTransform {
                                    id
                                }
                                ... on DraftOrder {
                                    id
                                }
                                ... on DiscountNode {
                                    id
                                }
                                ... on DiscountCodeNode {
                                    id
                                }
                                ... on DiscountAutomaticNode {
                                    id
                                }
                                ... on DeliveryCustomization {
                                    id
                                }
                                ... on CustomerSegmentMember {
                                    id
                                }
                                ... on Customer {
                                    id
                                }
                                ... on CompanyLocation {
                                    id
                                }
                                ... on Company {
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
    """
    return qry

def get_metafield_query_order():
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
                                ... on Validation {
                                    id
                                }
                                ... on Shop {
                                    id
                                    email
                                }
                                ... on ProductVariant {
                                    id
                                }
                                ... on Product {
                                    id
                                }
                                ... on PaymentCustomization {
                                    id
                                }
                                ... on Order {
                                    id
                                    email
                                }
                                ... on MediaImage {
                                    id
                                }
                                ... on Market {
                                    id
                                    name
                                }
                                ... on Location {
                                    id
                                    name
                                }
                                ... on Image {
                                    id
                                }
                                ... on FulfillmentConstraintRule {
                                    id
                                }
                                ... on AppInstallation {
                                    id
                                }
                                ... on CartTransform {
                                    id
                                }
                                ... on DraftOrder {
                                    id
                                }
                                ... on DiscountNode {
                                    id
                                }
                                ... on DiscountCodeNode {
                                    id
                                }
                                ... on DiscountAutomaticNode {
                                    id
                                }
                                ... on DeliveryCustomization {
                                    id
                                }
                                ... on CustomerSegmentMember {
                                    id
                                }
                                ... on Customer {
                                    id
                                }
                                ... on CompanyLocation {
                                    id
                                }
                                ... on Company {
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
    """
    return qry
