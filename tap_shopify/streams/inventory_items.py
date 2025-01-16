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
from tap_shopify.streams.graphql_queries import get_inventory_items_query

LOGGER = singer.get_logger()
RESULTS_PER_PAGE = 250

class InventoryItems(Stream):
    name = 'inventory_items'
    data_key = "inventoryItems"
    replication_key = "updated_at"

    @shopify_error_handling
    def call_api(self, query_params):
        query = get_inventory_items_query()
        LOGGER.info("Fetching %s", query_params)
        response = shopify.GraphQL().execute(query=query, variables=query_params, operation_name="GetinventoryItems")
        response = json.loads(response)
        if "errors" in response.keys():
            LOGGER.info("Error %s", response)
            raise Error("Unable to Query Data")
        data = response.get("data", {}).get(self.data_key, {})
        return data


    def transform_object(self, obj):
        """
        performs compatibility transformations
        TODO: Check Descrepancy in response data
        """
        obj["admin_graphql_api_id"] = obj["id"]
        obj["id"] = int(obj["id"].replace("gid://shopify/InventoryItem/", ""))
        obj["created_at"] = obj.get("createdAt")
        obj["updated_at"] = obj.get("updatedAt")
        unitCost = obj.get("unitCost")or {}
        obj["cost"] = unitCost.get("amount")
        obj["requires_shipping"] = obj.get("requiresShipping",)
        obj["country_code_of_origin"] = obj.get("countryCodeOfOrigin")
        obj["province_code_of_origin"] = obj.get("provinceCodeOfOrigin")
        country_harmonized_system_codes = []
        for edge in obj["countryHarmonizedSystemCodes"]["edges"]:
            LOGGER.info("Type %s", edge)
            item = edge.get("node", {})
            itx = {}
            itx["harmonized_system_code"] = item.get("harmonizedSystemCode", None)
            itx["country_code"] = item.get("countryCode", None)
            country_harmonized_system_codes.append(item)
        obj["harmonized_system_code"] = obj[ "harmonizedSystemCode"]
        obj["country_harmonized_system_codes"] = country_harmonized_system_codes

        return obj


    def get_query_params(self, updated_at_min, updated_at_max, cursor):
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
        """
        TODO: Check the bookmarking
        """
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
                # LOGGER.info("%s", data)
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
        """
        TODO: Change This
        """
        for obj in self.get_objects():
            yield obj


Context.stream_objects['inventory_items'] = InventoryItems
