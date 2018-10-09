import shopify
import singer

from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE

LOGGER = singer.get_logger()

class Orders(Stream):
    name = 'orders'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']

    def sync(self):
        orders_bookmark = self.get_bookmark()
        start_bookmark = self.query_start()
        count = 0

        def call_endpoint(page, start_date):
            return shopify.Order.find(
                # Max allowed value as of 2018-09-19 11:53:48
                limit=RESULTS_PER_PAGE,
                page=page,
                updated_at_min=start_date,
                updated_at_max=Context.tap_start,
                # Order is an undocumented query param that we believe
                # ensures the order of the results.
                order="updated_at asc")

        for order in self.paginate_endpoint(call_endpoint, start_bookmark):
            updated_at = utils.strptime_with_tz(order.updated_at)
            order_dict = order.to_dict()
            # Popping this because Transactions is its own stream
            order_dict.pop("transactions", [])
            if Context.is_selected(self.name) and updated_at >= orders_bookmark:
                count += 1
                yield (self.name, order_dict)

            for rec in self.sync_substreams(order, start_bookmark):
                yield rec

        LOGGER.info('Orders Count = %s', count)

Context.stream_objects['orders'] = Orders
Context.streams['orders'] = ['metafields', 'transactions']
