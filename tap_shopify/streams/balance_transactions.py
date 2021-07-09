import shopify
from tap_shopify.context import Context
from tap_shopify.streams.child_stream import ChildStream


class BalanceTransactions(ChildStream):
    name = 'transactions'
    replication_object = shopify.Transactions

    def get_parent_field_name(self):
        return 'payout_id'

    def get_parent_name(self):
        return 'payouts'


Context.stream_objects['balance_transactions'] = BalanceTransactions
