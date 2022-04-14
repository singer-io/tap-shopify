import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class DraftOrders(Stream):
    name = "draft_orders"
    replication_object = shopify.DraftOrder
    # needed because filter="any" does not work as expected
    # https://community.shopify.com/c/Shopify-APIs-SDKs/Draft-Order-API-status-any-stopped-working/td-p/557365
    status_key = "_"


Context.stream_objects["draft_orders"] = DraftOrders
