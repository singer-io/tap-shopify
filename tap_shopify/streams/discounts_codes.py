import shopify

from tap_shopify.context import Context
from tap_shopify.streams.child_stream import ChildStream

#Price Rules -> Child_stream.py -> Base.py
class DiscountCodes(ChildStream):
    name = 'discount_codes'
    replication_object = shopify.DiscountCode

    def get_parent_field_name(self):
        return 'price_rule_id'

    def get_parent_name(self):
        return 'price_rules'


Context.stream_objects['discount_codes'] = DiscountCodes
