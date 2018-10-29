import time
import math
import functools
import datetime
import pyactiveresource
import singer
from singer import utils
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 7

# This seems unnecessarily complicated. Rewriting it as a contextmanager
# doesn't work initially because of the looping logic which is forbidden
# in a context manager. There's probably another way to structure it so
# the looping is encapsulated with the decorator but the error handling is
# encapsulated with a contextmanager.
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
                        sleep_time_str = resp.headers.get('Retry-After') \
                                         or resp.headers.get('retry-after', 2)
                        LOGGER.info("Received 429 -- sleeping for %s seconds", sleep_time_str)
                        time.sleep(math.floor(float(sleep_time_str)))
                        continue
                    else:
                        LOGGER.error("Received a %s error.", resp.code)
                        raise
        return wrapped
    return decorator

class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']
    # Controls which SDK object we use to call the API by default.
    replication_object = None

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, bookmark_value):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.
        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            self.replication_key,
            bookmark_value
        )
        singer.write_state(Context.state)

    # This function can be overridden by subclasses for specialized API
    # interactions. If you override it you need to remember to decorate it
    # with shopify_error_handling to get 429 handling.
    @shopify_error_handling()
    def call_api(self, query_params):
        return self.replication_object.find(**query_params)

    def get_objects(self):

        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now()

        # Page through till the end of the resultset
        while updated_at_min < stop_time:
            page = 1
            updated_at_max = updated_at_min + datetime.timedelta(days=DATE_WINDOW_SIZE)
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            while True:
                query_params = {
                    "page": page,
                    "updated_at_min": updated_at_min,
                    "updated_at_max": updated_at_max,
                    "limit": RESULTS_PER_PAGE,
                    "status": "any"
                }
                objects = self.call_api(query_params)
                for obj in objects:
                    yield obj

                # You know you're at the end when the current page has
                # less than the request size limits you set.
                if len(objects) < RESULTS_PER_PAGE:
                    # Save the updated_at_max as our bookmark as we've synced all rows up in our
                    # window and can move forward
                    self.update_bookmark(utils.strftime(updated_at_max))
                    break

                page += 1
            updated_at_min = updated_at_max

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj.to_dict()
