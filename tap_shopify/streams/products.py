from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Products(Stream):
    """Stream class for Shopify Products"""

    name = "products"
    data_key = "products"
    replication_key = "updatedAt"

    def transform_object(self, obj):
        """
        Transforms the product object by extracting media information.

        Args:
            obj (dict): Product object.

        Returns:
            dict: Transformed product object.
        """
        media = obj.get("media")
        media_list = []
        if media and "edges" in media:
            for edge in media.get("edges"):
                node = edge.get("node")
                if node:
                    media_list.append(node)
        obj["media"] = media_list
        return obj

    def get_query(self):
        """
        Returns the GraphQL query to fetch all products.

        Returns:
            str: GraphQL query string.
        """
        return """
        query GetProducts($first: Int!, $after: String, $query: String) {
            products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
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


Context.stream_objects["products"] = Products
