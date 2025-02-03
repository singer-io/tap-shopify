from datetime import timedelta
import json
import urllib
import shopify

from singer import(
    metrics,
    get_logger,
    utils
)
from tap_shopify.context import Context
from tap_shopify.streams.base import (
    Stream,
    shopify_error_handling,
    DATE_WINDOW_SIZE,
    )


LOGGER = get_logger()

class ShopifyGraphQLError(Exception):
    """Custom exception for GraphQL errors"""


def execute_gql(self, query, variables=None, operation_name=None):
    """
    This overrides the `execute` method from ShopifyAPI(v12.6.0) to remove the print statement.
    Ensure to check the original impl before making any changes or upgrading the SDK version,
    as this modification may affect future updates
    """
    default_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    headers = self.merge_headers(default_headers, self.headers)
    data = {"query": query, "variables": variables, "operationName": operation_name}

    req = urllib.request.Request(self.endpoint, json.dumps(data).encode("utf-8"), headers)

    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise e

shopify.GraphQL.execute  = execute_gql

class ShopifyGqlStream(Stream):

    data_key = None

    def get_query(self):
        """
        Provides GraphQL query
        """
        raise NotImplementedError("Function Not Implemented")

    def transform_object(self, obj):
        """
        Modify this to perform custom transformation on each object
        """
        return obj

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        rkey = self.replication_key
        params = {
            "query": f"{rkey}:>='{updated_at_min}' AND {rkey}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    @shopify_error_handling
    def call_api(self, query_params):
        """
        - Modifies the default call api implementation to support GraphQL
        - Returns response Object dict
        """

        try:
            query = self.get_query()
            LOGGER.info("Fetching %s %s", self.name, query_params)
            response = shopify.GraphQL().execute(query=query, variables=query_params)
            response = json.loads(response)
            if "errors" in response.keys():
                raise ShopifyGraphQLError(response['errors'])
            data = response.get("data", {}).get(self.data_key, {})
            return data
        except ShopifyGraphQLError as gql_error:
            LOGGER.error("GraphQL Error %s", gql_error)
            raise ShopifyGraphQLError("An error occurred with the GraphQL API.") from gql_error
        except Exception as e:
            LOGGER.error("Unexpected error occurred.",)
            raise e

    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs
            - Pagination & Filtering of stream
            - Transformation and bookmarking
        """

        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=date_window_size)
            query_end = min(sync_start, date_window_end)
            has_next_page, cursor = True, None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)

                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                for edge in data.get("edges"):
                    obj = self.transform_object(edge.get("node"))
                    replication_value = utils.strptime_to_utc(obj[self.replication_key])
                    if replication_value > current_bookmark:
                        current_bookmark = replication_value
                    yield obj

                page_info =  data.get("pageInfo")
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            last_updated_at = query_end
            self.update_bookmark(utils.strftime(current_bookmark))

    def sync(self):
        """
        Default implementation for sync method
        """
        for obj in self.get_objects():
            yield obj
