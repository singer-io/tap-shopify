import shopify

from tap_shopify.context import Context

from tap_shopify.streams.child_stream import ChildStream


class OrderRefunds(ChildStream):
    name = 'order_refunds'
    replication_object = shopify.Refund
    replication_key = 'created_at'

    def get_parent_field_name(self):
        return 'order_id'

    def get_parent_name(self):
        return 'orders'

    def sync(self):
        for refund in self.get_objects():
            refund_dict = refund.to_dict()
            yield refund_dict


Context.stream_objects['order_refunds'] = OrderRefunds
