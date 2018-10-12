import shopify
import singer

from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()

class Transactions(Stream):
    name = 'transactions'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at' # Transactions are immutable?
    key_properties = ['id']

    def get_objects(self):
        # Get orders, bookmarking at `transactions`
        orders.name = "transactions"
        for order in orders.get_objects():
            # TODO do we need pagination here? Probably not?
            yield from order.transactions()

    def sync(self, parent_obj):
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            created_at = utils.strptime_with_tz(transaction_dict[self.replication_key])
            yield transaction_dict

Context.stream_objects['transactions'] = Transactions
