import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class DraftOrders(Stream):
    name = 'draft_orders'
    replication_object = shopify.DraftOrder
    status_value = "open,invoice_sent,completed"

Context.stream_objects['draft_orders'] = DraftOrders
