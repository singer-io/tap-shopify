from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)


def get_call_api_fn(obj):
    @shopify_error_handling()
    def call_api(page):
        # We always retrieve these wholesale since there's no obvious
        # way to bookmark them (the bookmark would only be valid
        # within the object)

        # FIXME Need to check if why these params cause an error
        return obj.transactions(
            #limit=RESULTS_PER_PAGE,
            #page=page,
            #order="updated_at asc"
        )
    return call_api


class Transactions(Stream):
    name = 'transactions'
    # Transactions have no updated_at property.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties
    replication_key = 'created_at'
    # FIXME bookmarking is likely wrong on transactions since we are not
    # overriding all the things we need to to to change it to
    # 'created_at'?

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
            # Override `call_api` to use the parent_object as the api
            # retrieval object.
            selected_parent.call_api = get_call_api_fn(parent_object)
            # Page through the specific parent_object's transactions
            yield from selected_parent.get_objects()

    def sync(self):
        # TODO What if we had a sync_hook member that took a dict and
        # yielded the processed version? That would reduce a bit more
        # duplication in the hierarchy (the base sync would take care of
        # calling get_objects and to_dict and the subclass would take care
        # _only_ of processing the dict).
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            created_at = utils.strptime_with_tz(transaction_dict[self.replication_key])
            yield transaction_dict

Context.stream_objects['transactions'] = Transactions
