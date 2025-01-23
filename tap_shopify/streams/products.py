
from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_products_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class Products(ShopifyGqlStream):
    name = 'products'
    data_key = "products"
    replication_key = "updatedAt"

    def get_query(self):
        return get_products_query()

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
        media = obj.get("media")
        media_list  = []
        if media and "edges" in media:
            for edge in media.get("edges"):
                node = edge.get("node")
                if node:
                    media_list.append(node)
        obj["media"] = media_list
        return obj

Context.stream_objects['products'] = Products
