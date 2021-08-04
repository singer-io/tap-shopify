import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.graph_ql_stream import GraphQlChildStream

LOGGER = singer.get_logger()


class CustomerJourney(GraphQlChildStream):
    name = 'customer_journey'

    replication_object = shopify.Order
    parent_key_access = "customerJourney"
    parent_name = "orders"
    parent_id_ql_prefix = 'gid://shopify/Order/'
    parent_per_page = 25
    child_is_list = False
    fragment_cols = {"moments": "CustomerVisit"}
    parent_replication_key = "createdAt"
    key_properties = []
    replication_key = "createdAt"


Context.stream_objects['customer_journey'] = CustomerJourney
