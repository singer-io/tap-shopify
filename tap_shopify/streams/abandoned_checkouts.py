import shopify
import singer
from  pyactiveresource.connection import ServerError
from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout

    @shopify_error_handling()
    def call_api(self, page):
        return self.replication_object.find(
            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            page=page,
            status='any',
            updated_at_min=self.get_bookmark(),
            # Order is an undocumented query param that we believe
            # ensures the order of the results.
            order="updated_at asc")

    def sync(self):
        for abandoned_checkout in self.get_objects():
            # TODO: Filter out customer and replace with ID? It can be
            # foreign keyed
            yield abandoned_checkout.to_dict()

Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
