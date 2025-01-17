import shopify
import json
import datetime

from singer import(
    metrics,
    get_logger,
    utils
)

from tap_shopify.streams.base import (
    Stream,
    shopify_error_handling,
    DATE_WINDOW_SIZE,
    )

from tap_shopify.context import Context

LOGGER = get_logger()

class ShopifyGraphQLError(Exception):
    """Custom exception for GraphQL errors"""
    pass

class ShopifyGqlStream(Stream):

    data_key = None

    def get_query(self):
        raise NotImplementedError("Function Not Implemented")

    def transform_object(self, obj):
        raise NotImplementedError("Function Not Implemented")

    def get_query_params(self, repl_key_min, repl_key_max, cursor=None):
        """
        Returns Query and pagination params for filtering 
        """
        params = {
            "query": f"{self.replication_key}:>'{repl_key_min}' {self.replication_key}:<'{repl_key_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    @shopify_error_handling
    def call_api(self, query_params):
        query = self.get_query()
        LOGGER.info("Fetching %s", query_params)
        response = shopify.GraphQL().execute(query=query, variables=query_params)
        response = json.loads(response)
        if "errors" in response.keys():
            raise ShopifyGraphQLError(response['errors'])
        data = response.get("data", {}).get(self.data_key, {})
        return data

    def get_objects(self):

        updated_at_min = self.get_bookmark()
        max_bookmark = updated_at_min
        stop_time = utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        while updated_at_min < stop_time:

            updated_at_max = min(updated_at_min + datetime.timedelta(days=date_window_size),stop_time)
            has_next_page, cursor = True, None

            while has_next_page:
                query_params = self.get_query_params(updated_at_min, updated_at_max, cursor)
                
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                for edge in data.get("edges"):
                    obj = self.transform_object(edge.get("node"))
                    replication_value = utils.strptime_to_utc(obj[self.replication_key])
                    if replication_value > max_bookmark:
                        max_bookmark = replication_value
                    yield obj

                page_info =  data.get("pageInfo")
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            updated_at_min = updated_at_max
            self.update_bookmark(utils.strftime(max_bookmark))

    def sync(self):
        """
        TODO: Update DocString
        """
        for obj in self.get_objects():
            yield obj