import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.graph_ql_stream import GraphQlChildStream

LOGGER = singer.get_logger()


class Transactions(GraphQlChildStream):
    name = 'transactions'

    replication_object = shopify.Transaction
    parent_key_access = "transactions"
    parent_name = "orders"
    parent_id_ql_prefix = 'gid://shopify/Order/'

    def transform_obj(self, obj):
        if "receipt" in obj and obj["receipt"] is not None:
            obj["receipt"] = obj["receipt"].replace("=>", ":").replace("nil", "null")
        return obj


Context.stream_objects['transactions'] = Transactions
