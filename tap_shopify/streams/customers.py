import shopify
import singer
from  pyactiveresource.connection import ServerError
from singer import utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream, RESULTS_PER_PAGE


class Customers(Stream):
    name = 'customers'
    replication_object = shopify.Customer

Context.stream_objects['customers'] = Customers
