import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)


class OrderRefunds(Stream):
    name = 'order_refunds'
    replication_object = shopify.Refund
    replication_key = 'created_at'

    @shopify_error_handling
    def get_refunds(self, parent_object, page):
        return self.replication_object.find(
            order_id=parent_object.id,
            limit=RESULTS_PER_PAGE,
            page=page)

    def get_objects(self):
        selected_parent = Context.stream_objects['orders']()
        selected_parent.name = "refund_orders"

        # Page through all `orders`, bookmarking at `refund_orders`
        for parent_object in selected_parent.get_objects():
            page = 1
            while True:
                refunds = self.get_refunds(parent_object, page)
                for refund in refunds:
                    yield refund
                if len(refunds) < RESULTS_PER_PAGE:
                    break
                page += 1

    def sync(self):
        for refund in self.get_objects():
            refund_dict = refund.to_dict()
            yield refund_dict

Context.stream_objects['order_refunds'] = OrderRefunds
