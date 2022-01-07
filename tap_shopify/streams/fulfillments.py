import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

LOGGER = singer.get_logger()

FULFILLMENT_RESULTS_PER_PAGE = 100


class Fulfillments(Stream):
    name = 'fulfillments'
    replication_key = 'created_at'
    replication_object = shopify.Fulfillment

    @shopify_error_handling
    def call_api_for_fulfillments(self, parent_object):
        return self.replication_object.find(
            limit=FULFILLMENT_RESULTS_PER_PAGE,
            order_id=parent_object.id,
        )

    def get_fulfillments(self, parent_object):
        page = self.call_api_for_fulfillments(parent_object)
        yield from page

        while page.has_next_page():
            page = page.next_page()
            yield from page

    def get_objects(self):
        selected_parent = Context.stream_objects['orders']()
        selected_parent.name = "fulfillment_orders"

        for parent_object in selected_parent.get_objects():
            fulfillments = self.get_fulfillments(parent_object)
            for fulfillment in fulfillments:
                yield fulfillment

    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for fulfillment in self.get_objects():
            fulfillment_dict = fulfillment.to_dict()
            replication_value = strptime_to_utc(fulfillment_dict[self.replication_key])
            if replication_value >= bookmark:
                yield fulfillment_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value
        self.update_bookmark(strftime(max_bookmark))


Context.stream_objects['fulfillments'] = Fulfillments
