import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.graph_ql_stream import GraphQlChildStream

LOGGER = singer.get_logger()


class CustomerVisit(GraphQlChildStream):
    name = 'customer_visit'

    replication_object = shopify.Order
    parent_key_access = "customerJourney"
    parent_name = "orders"
    parent_id_ql_prefix = 'gid://shopify/Order/'
    parent_per_page = 25
    node_argument = False
    # should be represented by one relation key-value in dict
    inline_fragments = {'moments': 'CustomerVisit'}


Context.stream_objects['customer_visit'] = CustomerVisit
