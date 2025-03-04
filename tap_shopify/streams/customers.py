from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_customers_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class Customers(ShopifyGqlStream):
    name = 'customers'
    data_key = "customers"
    replication_key = "updatedAt"

    def get_query(self):
        return get_customers_query()

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

Context.stream_objects['customers'] = Customers
