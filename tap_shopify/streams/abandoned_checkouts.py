import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout

    @shopify_error_handling()
    def call_api(self, page, bookmark):
        return self.replication_object.find(

            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            page=page,
            updated_at_min=bookmark,

            # Order is an undocumented query param that we believe ensures
            # the order of the results.
            order="updated_at asc",

            # https://help.shopify.com/en/api/reference/orders/abandoned_checkouts#endpoints
            # says that the `status` parameter defaults to 'open'. We want
            # all abandoned checkouts. 'any' is an undocumented value that
            # we believe results in both 'open' and 'closed' checkouts
            # being retrieved.
            status='any')

    def sync(self):
        for abandoned_checkout in self.get_objects():
            abandoned_checkout_dict = abandoned_checkout.to_dict()
            # Customer is a stream on its own that can be foreign keyed to
            customer = abandoned_checkout.pop('customer')
            abandoned_checkout_dict['customer_id'] = customer['id']
            yield abandoned_checkout_dict

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
