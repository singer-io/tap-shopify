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
    key_properties = ['id']

    def sync(self):
        start_bookmark = self.query_start()
        count = 0

        def should_giveup_retry(exception):
            return exception.code < 500

        @utils.backoff(ServerError,
                        giveup=should_giveup_retry)
        def call_endpoint(page, start_date):
            return shopify.Customer.find(
                # Max allowed value as of 2018-09-19 11:53:48
                limit=RESULTS_PER_PAGE,
                page=page,
                status='any',
                updated_at_min=start_date,
                updated_at_max=Context.tap_start,
                # Order is an undocumented query param that we believe
                # ensures the order of the results.
                order="updated_at asc")

        for customer in self.paginate_endpoint(call_endpoint, start_bookmark):
            count += 1
            yield (self.name, customer.to_dict())

        LOGGER.info('Customer Count = %s', count)

Context.stream_objects['customers'] = Customers
Context.streams['customers'] = []
