from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class ProductVariants(Stream):
    """Stream class for Product Variants in Shopify."""
    name = "product_variants"
    data_key = "productVariants"
    replication_key = "updatedAt"

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
