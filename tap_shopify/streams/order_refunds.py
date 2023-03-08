import shopify
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling,
                                      OutOfOrderIdsError,
                                      canonicalize)

class OrderRefunds(Stream):
    name = 'order_refunds'
    replication_object = shopify.Refund
    replication_key = 'created_at'

    @shopify_error_handling
    def get_refunds(self, parent_object, since_id):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        return self.replication_object.find(
            order_id=parent_object.id,
            limit=self.results_per_page,
            since_id=since_id,
            order='id asc')

    def get_objects(self):
        selected_parent = Context.stream_objects['orders']()
        selected_parent.name = "refund_orders"

        # Page through all `orders`, bookmarking at `refund_orders`
        for parent_object in selected_parent.get_objects():
            since_id = 1
            while True:
                refunds = self.get_refunds(parent_object, since_id)
                for refund in refunds:
                    if refund.id < since_id:
                        raise OutOfOrderIdsError("refund.id < since_id: {} < {}".format(
                            refund.id, since_id))
                    yield refund
                if len(refunds) < self.results_per_page:
                    break
                if refunds[-1].id != max([o.id for o in refunds]):
                    raise OutOfOrderIdsError("{} is not the max id in refunds ({})".format(
                        refunds[-1].id, max([o.id for o in refunds])))
                since_id = refunds[-1].id

    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for refund in self.get_objects():
            refund_dict = refund.to_dict()
            replication_value = strptime_to_utc(refund_dict[self.replication_key])
            if replication_value >= bookmark:
                for transaction_dict in refund_dict.get("transactions",[]):
                    for field_name in ['token', 'version', 'ack', 'timestamp', 'build']:
                        canonicalize(transaction_dict, field_name)
                yield refund_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(strftime(max_bookmark))


Context.stream_objects['order_refunds'] = OrderRefunds
