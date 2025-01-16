"""
Stores all the GraphQl Queries for shopify api
"""

def get_products_query():
    """
    product stream get query
    """
    return """
        query GetProducts($first: Int!, $after: String, $query: String) {
            products(first: $first, after: $after, query: $query)
            {
                edges {
                    node {
                        id
                        title
                        descriptionHtml
                        vendor
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
                            countryHarmonizedSystemCodes(first: 250) {
                                edges {
                                    node {
                                        harmonizedSystemCode
                                        countryCode
                                    }
                                }
                            }
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
