import time
import datetime
import math

import pyactiveresource
import functools
import singer
from singer import utils
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

def shopify_error_handling():
    def decorator(fnc):
        @functools.wraps(fnc)
        def wrapped(*args, **kwargs):
            # Shopify returns 429s when their leaky bucket rate limiting
            # algorithm has been tripped. This will retry those
            # indefinitely. At some point we could consider adding a
            # max_retry configuration into this loop. So far that has
            # proven unnecessary
            while True:
                try:
                    return fnc(*args, **kwargs)
                except pyactiveresource.connection.ClientError as client_error:
                    # We have never seen this be anything _but_ a 429. Other
                    # states should be consider untested.
                    resp = client_error.response
                    if resp.code == 429:
                        # Retry-After is an undocumented header. But honoring
                        # it was proven to work in our spikes.
                        sleep_time_str = resp.headers['Retry-After']
                        LOGGER.info("Received 429 -- sleeping for %s seconds", sleep_time_str)
                        time.sleep(math.floor(float(sleep_time_str)))
                        continue
                    else:
                        LOGGER.ERROR("Received a %s error.", resp.code)
                        raise
        return wrapped
    return decorator

class Stream():
    name = None
    replication_method = None
    replication_key = None
    replication_object = None
    key_properties = None

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, obj):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.

        # Assuming that ordering works for bookmarking
        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            # All bookmarkable streams bookmark `updated_at`
            # FIXME remove configurable replication_key from all the
            # things
            'updated_at',
            obj.updated_at)
        singer.write_state(Context.state)

    @shopify_error_handling()
    def call_api(self, page):
        return self.replication_object.find(
            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            # TODO do we need `status='any'` here or
            # something? See abandoned_checkouts
            page=page,
            updated_at_min=self.get_bookmark(),
            # Order is an undocumented query param that we believe
            # ensures the order of the results.
            order="updated_at asc")

    def get_objects(self):
        page = 1
        # Page through till the end of the resultset
        while True:
            count = 0

            # `call_api` is set above for the default case and overridden
            # for sub classes that are responsible for substreams on other
            # objects.
            objects = self.call_api(page)

            for obj in objects:
                self.update_bookmark(obj)
                yield obj
                count += 1

            LOGGER.info('%s Count = %s', self.name, count)

            if len(objects) < RESULTS_PER_PAGE:
                # You know you're at the end when the current page has
                # less than the request size limits you set.
                break
            page += 1
