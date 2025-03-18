
from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream



class ProductVariants(ShopifyGqlStream):
    name = 'product_variants'
    data_key = "productVariants"
    replication_key = "updatedAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params


    def transform_object(self, obj):
        return obj

    def get_query(self):
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

Context.stream_objects['product_variants'] = ProductVariants
