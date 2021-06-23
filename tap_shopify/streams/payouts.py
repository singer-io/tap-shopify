import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context
import datetime


class Payouts(Stream):
    name = 'payouts'
    replication_object = shopify.Payouts
    add_status = False
    skip_day = True
    def get_max_replication_key(self):
        return "date_max"

    def get_min_replication_key(self):
        return "date_min"


Context.stream_objects['payouts'] = Payouts
