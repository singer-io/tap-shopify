
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_product_variant_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class ProductVariants(ShopifyGqlStream):
    name = 'product_variants'
    data_key = "productVariants"
    replication_key = "updatedAt"

    def get_query(self):
        return get_product_variant_query()

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

Context.stream_objects['product_variants'] = ProductVariants
