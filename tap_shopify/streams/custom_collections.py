import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling,
                                      OutOfOrderIdsError)


class CustomCollections(Stream):
    name = 'custom_collections'
    replication_object = shopify.CustomCollection
    replication_key = 'updated_at'

    def get_objects(self):
        'Paginate the return and add collection_id to returned product objects'
        page = self.replication_object.find()
        for collection in page:
            for product in collection.products():
                edit_product = product.to_dict()
                edit_product["collection_id"] = collection.id
                yield edit_product
        if page.has_next_page():
            page = page.next_page()

    def sync(self):
        for product in self.get_objects():
            yield product

Context.stream_objects['custom_collections'] = CustomCollections