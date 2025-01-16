
from tap_shopify.context import Context
from tap_shopify.streams.graphql.gql_queries import get_products_query
from tap_shopify.streams.graphql.gql_base import ShopifyGqlStream



class Products(ShopifyGqlStream):
    name = 'products'
    data_key = "products"
    replication_key = "updated_at"

    get_query = get_products_query

    def transform_object(self, obj):
        """
        performs compatibility transformations
        TODO: Check Descrepancy in response data
        TODO: Error Handling
        """
        obj["admin_graphql_api_id"] = obj["id"]
        obj["id"] = int(obj["id"].replace("gid://shopify/Product/", ""))
        opts = []
        for item in obj["options"]:
            item["id"] = int(item["id"].replace("gid://shopify/ProductOption/", ""))
            item["product_id"] = obj["id"]
            opts.append(item)
        obj["options"] = opts
        obj["published_at"] = obj[ "publishedAt"]
        obj["created_at"] = obj[ "createdAt"]
        obj["updated_at"] = obj[ "updatedAt"]
        obj["body_html"] = obj[ "descriptionHtml"]
        obj["product_type"] = obj[ "productType"]
        obj["template_suffix"] = obj[ "templateSuffix"]

        return obj

Context.stream_objects['products'] = Products
