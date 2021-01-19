import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)

from tap_shopify.streams.graph_ql_stream import GraphQlChildStream
LOGGER = singer.get_logger()

class Transactions(GraphQlChildStream):
    name = 'transactions'

    replication_object = shopify.Transaction
    parent_key_access = "transactions"
    parent_name = "orders"


Context.stream_objects['transactions'] = Transactions
