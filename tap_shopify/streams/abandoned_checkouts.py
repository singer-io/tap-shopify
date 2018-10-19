import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout

    def sync(self):
        for abandoned_checkout in self.get_objects(status="any"):
            abandoned_checkout_dict = abandoned_checkout.to_dict()
            # Customer is a stream on its own that can be foreign keyed to
            if abandoned_checkout_dict.get('customer'):
                customer = abandoned_checkout_dict.pop('customer')
                abandoned_checkout_dict['customer_id'] = customer['id']
            yield abandoned_checkout_dict

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
