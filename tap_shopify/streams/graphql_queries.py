

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