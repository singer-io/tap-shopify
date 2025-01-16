import json
import datetime
import shopify

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

from tap_shopify.streams.base import (
    Stream,
    shopify_error_handling,
    DATE_WINDOW_SIZE,
    Error
    )
from tap_shopify.context import Context
from tap_shopify.streams.graphql_queries import get_products_query

LOGGER = singer.get_logger()

class Products(Stream):
    name = 'products'
    data_key = 'products'
    replication_key = "updated_at"

    @shopify_error_handling
    def call_api(self, query_params):
        query = get_products_query()
        LOGGER.info("Fetching %s", query_params)
        response = shopify.GraphQL().execute(query=query, variables=query_params, operation_name="GetProducts")
        response = json.loads(response)
        if "errors" in response.keys():
            raise Error("Unable to Query Data")
        data = response.get("data", {}).get(self.data_key, {})
        return data


    def transform_object(self, obj):
        """
        performs compatibility transformations
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


    def get_query_params(self, updated_at_min, updated_at_max, cursor, *args, **kwargs):
        """
        Returns Query and pagination params for filtering 
        """
        params = {
            "query": f"updated_at:>'{updated_at_min}' updated_at:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def get_objects(self):
        last_sync_interrupted_at = self.get_updated_at_max()
        updated_at_min = self.get_bookmark()
        max_bookmark = updated_at_min
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        while updated_at_min < stop_time:

            updated_at_max = (last_sync_interrupted_at
                              or updated_at_min + datetime.timedelta(days=date_window_size))

            last_sync_interrupted_at = None
            cursor = None
            updated_at_max = min(updated_at_max, stop_time)

            while True:
                query_params = self.get_query_params(updated_at_min, updated_at_max, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                pageInfo =  data.get("pageInfo")
                endCursor = pageInfo.get("endCursor")

                for edge in data.get("edges"):
                    obj = self.transform_object(edge.get("node"))
                    replication_value = strptime_to_utc(obj[self.replication_key])
                    if replication_value > max_bookmark:
                        max_bookmark = replication_value
                    yield obj

                if not pageInfo.get("hasNextPage"):
                    stream_bookmarks = Context.state.get('bookmarks', {}).get(self.name, {})
                    stream_bookmarks.pop('updated_at_max', None)
                    self.update_bookmark(utils.strftime(updated_at_max))
                    break

                cursor = endCursor
                self.update_bookmark(utils.strftime(updated_at_max), bookmark_key='updated_at_max')

            updated_at_min = updated_at_max

        bookmark = max(min(stop_time,
                           max_bookmark),
                       (stop_time - datetime.timedelta(days=date_window_size)))
        self.update_bookmark(utils.strftime(bookmark))

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj


Context.stream_objects['products'] = Products
