from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream, get_locations_query



class Locations(ShopifyGqlStream):
    name = 'locations'
    data_key = "locations"
    replication_key = "createdAt"

    def get_query(self):
        return get_locations_query()

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "created_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

Context.stream_objects['locations'] = Locations
