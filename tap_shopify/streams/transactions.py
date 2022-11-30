import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling,
                                      canonicalize)

LOGGER = singer.get_logger()

# https://help.shopify.com/en/api/reference/orders/transaction An
# order can have no more than 100 transactions associated with it.
TRANSACTIONS_RESULTS_PER_PAGE = 100


class Transactions(Stream):
    name = 'transactions'
    replication_key = 'created_at'
    replication_object = shopify.Transaction
    # Added decorator over functions of shopify SDK
    replication_object.find = shopify_error_handling(replication_object.find)
    # Transactions have no updated_at property. Therefore we have
    # nothing to set the `replication_method` member to.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties

    def call_api_for_transactions(self, parent_object):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        return self.replication_object.find(
            limit=TRANSACTIONS_RESULTS_PER_PAGE,
            order_id=parent_object.id,
        )

    def get_transactions(self, parent_object):
        # We do not need to support paging on this substream. If that
        # were to become untrue, reference Metafields.
        #
        # We do not user the `transactions` method of the order object
        # like in metafield because they overrode it here to not
        # support limit overrides.
        #
        # https://github.com/Shopify/shopify_python_api/blob/e8c475ccc84b1516912b37f691d00ecd24921e9b/shopify/resources/order.py#L17-L18

        page = self.call_api_for_transactions(parent_object)
        yield from page

        while page.has_next_page():
            page = page.next_page()
            yield from page

    def get_objects(self):
        # Right now, it's ok for the user to select 'transactions' but not
        # 'orders'. This data may not be all that useful but we're taking
        # the less opinionated approach to begin with to favor simplicity.
        # This is where you would need to add the behavior for enforcing
        # that 'orders' is selected if we want to go that route in the
        # future.

        # Get transactions, bookmarking at `transaction_orders`
        selected_parent = Context.stream_objects['orders']()
        selected_parent.name = "transaction_orders"

        # Page through all `orders`, bookmarking at `transaction_orders`
        for parent_object in selected_parent.get_objects():
            transactions = self.get_transactions(parent_object)
            for transaction in transactions:
                yield transaction

    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            replication_value = strptime_to_utc(transaction_dict[self.replication_key])
            if replication_value >= bookmark:
                for field_name in ['token', 'version', 'ack', 'timestamp', 'build']:
                    canonicalize(transaction_dict, field_name)
                yield transaction_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(strftime(max_bookmark))

Context.stream_objects['transactions'] = Transactions
