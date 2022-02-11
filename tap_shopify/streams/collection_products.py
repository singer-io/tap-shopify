import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling,
                                      OutOfOrderIdsError)


class CollectionProducts(Stream):
    name = 'collection_products'
    replication_object = shopify.CustomCollection
    replication_key = 'updated_at'

    @shopify_error_handling
    def get_collection_products(self, collection):
        return collection.products()

    @shopify_error_handling
    def get_collections_products(self):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        return self.replication_object.find()

    @shopify_error_handling
    def get_next_page(self, page):
        return page.next_page()

    def get_objects(self):
        
        while True:
            page = self.get_collections_products()
            for collection in page:
                for product in self.get_collection_products(collection):
                    yield product
            if page.has_next_page():
                page = self.get_next_page(page)
            else:
                break

    def sync(self):
        for product in self.get_objects():
            yield product.to_dict()

Context.stream_objects['collection_products'] = CollectionProducts