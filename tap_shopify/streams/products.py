
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_products_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class Products(ShopifyGqlStream):
    name = 'products'
    data_key = "products"
    replication_key = "updatedAt"
    get_query = get_products_query


Context.stream_objects['products'] = Products
