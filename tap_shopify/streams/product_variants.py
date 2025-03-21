from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class ProductVariants(ShopifyGqlStream):
    """Stream class for Product Variants in Shopify."""
    name = "product_variants"
    data_key = "productVariants"
    replication_key = "updatedAt"

    # pylint: disable=arguments-differ
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering and pagination.

        Args:
            updated_at_min (str): Minimum updated_at timestamp for filtering.
            updated_at_max (str): Maximum updated_at timestamp for filtering.
            cursor (str, optional): Cursor for pagination.

        Returns:
            dict: Query parameters.
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
        """
        Transforms the object if needed.

        Args:
            obj (dict): The object to transform.

        Returns:
            dict: The transformed object.
        """
        return obj

    def get_query(self):
        """
        Returns the GraphQL query to get all product variants.

        Returns:
            str: The GraphQL query string.
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


Context.stream_objects["product_variants"] = ProductVariants
