import os
import sys
import shopify
import singer
import json
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

LOGGER = singer.get_logger()


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class IncomingItems(Stream):
    name = 'incoming_items'
    replication_key = 'createdAt'
    gql_query = "query inventoryLevel($id: ID!){inventoryLevel(id: $id){id, incoming, createdAt}}"

    @shopify_error_handling
    def call_api_for_incoming_items(self, parent_object):
        gql_client = shopify.GraphQL()
        with HiddenPrints():
            response = gql_client.execute(self.gql_query, dict(id=parent_object.admin_graphql_api_id))
        return json.loads(response)

    def get_objects(self):
        selected_parent = Context.stream_objects['inventory_levels']()
        selected_parent.name = "inventory_levels"

        for parent_object in selected_parent.get_objects():
            incoming_item = self.call_api_for_incoming_items(parent_object)
            yield incoming_item["data"].get("inventoryLevel")

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        for incoming_item in self.get_objects():
            replication_value = strptime_to_utc(incoming_item[self.replication_key])
            if replication_value >= bookmark:
                yield incoming_item
            if replication_value > self.max_bookmark:
                self.max_bookmark = replication_value

        self.update_bookmark(strftime(self.max_bookmark))


Context.stream_objects['incoming_items'] = IncomingItems
