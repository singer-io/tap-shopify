import shopify
import singer

from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)

class Transactions(Stream):
    name = 'transactions'
    # Transactions have no updated_at property.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties
    replication_key = 'created_at'

    def get_selected_parents(self):
        # FIXME note all other parents
        for parent_stream in ['orders']:
            if Context.is_selected(parent_stream):
                yield Context.stream_objects[parent_stream]()

    def get_call_api_fn(self, obj):
        @shopify_error_handling()
        def call_api(page):
            # We always retrieve these wholesale since there's no obvious
            # way to bookmark them (the bookmark would only be valid
            # within the object)

            # Note: Need to check if why these params cause an error
            return obj.transactions(
                #limit=RESULTS_PER_PAGE,
                #page=page,
                #order="updated_at asc"
            )
        return call_api

    def get_objects(self):
        # Get orders, bookmarking at `transactions`
        for selected_parent in self.get_selected_parents():
            selected_parent.name = "transaction_{}".format(selected_parent.name)
            for parent_object in selected_parent.get_objects():
                selected_parent.call_api = self.get_call_api_fn(parent_object)
                yield from selected_parent.get_objects()

    def sync(self):
        for transaction in self.get_objects():
            transaction_dict = transaction.to_dict()
            created_at = utils.strptime_with_tz(transaction_dict[self.replication_key])
            yield transaction_dict

Context.stream_objects['transactions'] = Transactions
