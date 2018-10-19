from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

@shopify_error_handling()
def get_transactions(parent_object):
    return parent_object.transactions()

class Transactions(Stream):
    name = 'transactions'
    replication_key = 'created_at'
    # Transactions have no updated_at property.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties

    def get_objects(self, status="open"):
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
            transactions = get_transactions(parent_object)
            for transaction in transactions:
                yield transaction

    def sync(self):
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            yield transaction_dict

Context.stream_objects['transactions'] = Transactions
