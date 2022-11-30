import datetime
import functools
import math
import sys
import socket
import backoff
import pyactiveresource
import pyactiveresource.formats
import simplejson
import singer
from singer import metrics, utils
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 175

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 1

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5

# We have observed transactions with receipt objects that contain both:
#   - `token` and `Token`
#   - `version` and `Version`
#   - `ack` and `Ack`
# keys on transactions where PayPal is the payment type. We reached out to
# PayPal support and they told us the values should be the same, so one
# can be safely ignored since its a duplicate. Example: The logic is to
# prefer `token` if both are present and equal, convert `Token` -> `token`
# if only `Token` is present, and throw an error if both are present and
# their values are not equal.
def canonicalize(transaction_dict, field_name):
    field_name_upper = field_name.capitalize()
    # Not all Shopify transactions have receipts. Facebook has been shown
    # to push a null receipt through the transaction
    receipt = transaction_dict.get('receipt', {})
    if receipt:
        value_lower = receipt.get(field_name)
        value_upper = receipt.get(field_name_upper)
        if value_lower and value_upper:
            if value_lower == value_upper:
                LOGGER.info((
                    "Transaction (id=%d) contains a receipt "
                    "that has `%s` and `%s` keys with the same "
                    "value. Removing the `%s` key."),
                            transaction_dict['id'],
                            field_name,
                            field_name_upper,
                            field_name_upper)
                transaction_dict['receipt'].pop(field_name_upper)
            else:
                raise ValueError((
                    "Found Transaction (id={}) with a receipt that has "
                    "`{}` and `{}` keys with the different "
                    "values. Contact Shopify/PayPal support.").format(
                        transaction_dict['id'],
                        field_name_upper,
                        field_name))
        elif value_upper:
            # pylint: disable=line-too-long
            transaction_dict["receipt"][field_name] = transaction_dict['receipt'].pop(field_name_upper)

# function to return request timeout
def get_request_timeout():

    request_timeout = REQUEST_TIMEOUT # set default timeout
    timeout_from_config = Context.config.get('request_timeout')
    # updated the timeout value if timeout is passed in config and not from 0, "0", ""
    if timeout_from_config and float(timeout_from_config):
        # update the request timeout for the requests
        request_timeout = float(timeout_from_config)

    return request_timeout

def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'code', None) and exc.code not in status_code:
            return True
        # Retry other errors up to the max
        return False
    return gen_fn

def leaky_bucket_handler(details):
    LOGGER.info("Received 429 -- sleeping for %s seconds",
                details['wait'])

def retry_handler(details):
    LOGGER.info("Received 500 or retryable error -- Retry %s/%s",
                details['tries'], MAX_RETRIES)

#pylint: disable=unused-argument
def retry_after_wait_gen(**kwargs):
    # This is called in an except block so we can retrieve the exception
    # and check it.
    exc_info = sys.exc_info()
    resp = exc_info[1].response
    # Retry-After is an undocumented header. But honoring
    # it was proven to work in our spikes.
    # It's been observed to come through as lowercase, so fallback if not present
    sleep_time_str = resp.headers.get('Retry-After', resp.headers.get('retry-after'))
    yield math.floor(float(sleep_time_str))

# boolean function to check if the error is 'timeout' error or not
def is_timeout_error(error_raised):
    """
        This function checks whether the error contains 'timed out' substring and return boolean
        values accordingly, to decide whether to backoff or not.
    """
    # retry if the error string contains 'timed out'
    if str(error_raised).__contains__('timed out'):
        return False
    return True

def shopify_error_handling(fnc):
    @backoff.on_exception(backoff.expo, # timeout error raise by Shopify
                          (pyactiveresource.connection.Error, socket.timeout),
                          giveup=is_timeout_error,
                          max_tries=MAX_RETRIES,
                          factor=2)
    @backoff.on_exception(backoff.expo,
                          (pyactiveresource.connection.ServerError,
                           pyactiveresource.formats.Error,
                           simplejson.scanner.JSONDecodeError),
                          giveup=is_not_status_code_fn(range(500, 599)),
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(retry_after_wait_gen,
                          pyactiveresource.connection.ClientError,
                          giveup=is_not_status_code_fn([429]),
                          on_backoff=leaky_bucket_handler,
                          # No jitter as we want a constant value
                          jitter=None)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

class Error(Exception):
    """Base exception for the API interaction module"""

class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""

class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']
    # Controls which SDK object we use to call the API by default.
    replication_object = None
    # Status parameter override option
    status_key = None
    results_per_page = None

    def __init__(self):
        self.results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)

        # set request timeout
        self.request_timeout = get_request_timeout()

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def get_since_id(self):
        return singer.get_bookmark(Context.state,
                                   # name is overridden by some substreams
                                   self.name,
                                   'since_id')

    def update_bookmark(self, bookmark_value, bookmark_key=None):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.
        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            bookmark_key or self.replication_key,
            bookmark_value
        )
        singer.write_state(Context.state)


    # This function can be overridden by subclasses for specialized API
    # interactions. If you override it you need to remember to decorate it
    # with shopify_error_handling to get 429 and 500 handling.
    @shopify_error_handling
    def call_api(self, query_params):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        return self.replication_object.find(**query_params)

    def get_query_params(self, since_id, status_key, updated_at_min, updated_at_max):
        return {
            "since_id": since_id,
            "updated_at_min": updated_at_min,
            "updated_at_max": updated_at_max,
            "limit": self.results_per_page,
            status_key: "any"
        }

    def get_objects(self):
        updated_at_min = self.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Page through till the end of the resultset
        while updated_at_min < stop_time:
            # Bookmarking can also occur on the since_id
            since_id = self.get_since_id() or 1

            if since_id != 1:
                LOGGER.info("Resuming sync from since_id %d", since_id)

            # It's important that `updated_at_min` has microseconds
            # truncated. Why has been lost to the mists of time but we
            # think it has something to do with how the API treats
            # microseconds on its date windows. Maybe it's possible to
            # drop data due to rounding errors or something like that?
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            while True:
                status_key = self.status_key or "status"
                query_params = self.get_query_params(since_id,
                                                     status_key,
                                                     updated_at_min,
                                                     updated_at_max)

                with metrics.http_request_timer(self.name):
                    objects = self.call_api(query_params)

                for obj in objects:
                    if obj.id < since_id:
                        # This verifies the api behavior expectation we
                        # have that all results actually honor the
                        # since_id parameter.
                        raise OutOfOrderIdsError("obj.id < since_id: {} < {}".format(
                            obj.id, since_id))
                    yield obj

                # You know you're at the end when the current page has
                # less than the request size limits you set.
                if len(objects) < self.results_per_page:
                    # Save the updated_at_max as our bookmark as we've synced all rows up in our
                    # window and can move forward. Also remove the since_id because we want to
                    # restart at 1.
                    Context.state.get('bookmarks', {}).get(self.name, {}).pop('since_id', None)
                    self.update_bookmark(utils.strftime(updated_at_max))
                    break

                if objects[-1].id != max([o.id for o in objects]):
                    # This verifies the api behavior expectation we have
                    # that all pages are internally ordered by the
                    # `since_id`.
                    raise OutOfOrderIdsError("{} is not the max id in objects ({})".format(
                        objects[-1].id, max([o.id for o in objects])))
                since_id = objects[-1].id

                # Put since_id into the state.
                self.update_bookmark(since_id, bookmark_key='since_id')

            updated_at_min = updated_at_max

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj.to_dict()
