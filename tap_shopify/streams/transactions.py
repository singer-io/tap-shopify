import shopify
import singer

from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import SubStream

LOGGER = singer.get_logger()

class Transactions(SubStream):
    name = 'transactions'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at' # Transactions are immutable?
    key_properties = ['id']

    def sync(self, parent_obj):
        count = 0

        def call_endpoint(_1, _2):
            return shopify.Transaction.find(order_id=parent_obj.id)

        for transaction in self.paginate_endpoint(call_endpoint, None):
            transaction_dict = transaction.to_dict()
            created_at = utils.strptime_with_tz(transaction_dict[self.replication_key])
            count += 1
            yield (self.name, transaction_dict)

        LOGGER.info("Transactions Count = %s", count)

Context.stream_objects['transactions'] = Transactions
