import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

# https://help.shopify.com/en/api/reference/orders/transaction An
# order can have no more than 100 transactions associated with it.
TRANSACTIONS_RESULTS_PER_PAGE = 100

class Transactions(Stream):
    name = 'transactions'
    replication_key = 'created_at'
    replication_object = shopify.Transaction
    # Transactions have no updated_at property. Therefore we have
    # nothing to set the `replication_method` member to.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties

    @shopify_error_handling
    def get_transactions(self, parent_object):
        # We do not need to support paging on this substream. If that
        # were to become untrue, reference Metafields.
        #
        # We do not user the `transactions` method of the order object
        # like in metafield because they overrode it here to not
        # support limit overrides.
        #
        # https://github.com/Shopify/shopify_python_api/blob/e8c475ccc84b1516912b37f691d00ecd24921e9b/shopify/resources/order.py#L17-L18
        return self.replication_object.find(
            limit=TRANSACTIONS_RESULTS_PER_PAGE, order_id=parent_object.id)

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
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            yield transaction_dict

Context.stream_objects['transactions'] = Transactions
