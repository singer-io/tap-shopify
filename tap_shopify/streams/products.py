
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_products_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class Products(ShopifyGqlStream):
    name = 'products'
    data_key = "products"
    replication_key = "updated_at"

    def get_query(self):
        return get_products_query()

    def transform_object(self, obj):
        obj["updated_at"] = obj["updatedAt"]
        return obj

Context.stream_objects['products'] = Products
