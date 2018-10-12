import shopify
import singer
from  pyactiveresource.connection import ServerError
from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE

LOGGER = singer.get_logger()

class Customers(Stream):
    name = 'customers'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    replication_object = shopify.Customer
    key_properties = ['id']

    def sync(self):
        for customer in self.get_objects():
            yield customer.to_dict()

Context.stream_objects['customers'] = Customers
