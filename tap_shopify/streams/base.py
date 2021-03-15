import datetime
import functools
import math
import os
import sys

import backoff
import pyactiveresource
import pyactiveresource.formats
import simplejson
import singer
from singer import metrics, utils
from tap_shopify.context import Context
import shopify
import json
import pyactiveresource.connection
from socket import error as SocketError
from http.client import IncompleteRead
from tap_shopify.remember_errors_backoff import RememberErrorsBackoff

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 1

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 10


def is_status_code_fn(blacklist=None, whitelist=None):
    def gen_fn(exc):
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False

        if blacklist is not None and status_code not in blacklist:
            return True

        if whitelist is not None and status_code in whitelist:
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


# pylint: disable=unused-argument
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


remember_errors = RememberErrorsBackoff()

def shopify_error_handling(fnc):
    @backoff.on_exception(backoff.expo,
                          (pyactiveresource.connection.Error,
                           pyactiveresource.formats.Error,
                           simplejson.scanner.JSONDecodeError,
                           SocketError,
                           IncompleteRead,
                           ConnectionResetError,
                           GraphQLGeneralError),
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES,
                          giveup=is_status_code_fn(whitelist=[401, 403]),
                          jitter=None,
                          max_value=60)
    @backoff.on_exception(remember_errors.get_yield,
                          (pyactiveresource.connection.ClientError,
                           GraphQLThrottledError),
                          giveup=is_status_code_fn(blacklist=[429]),
                          on_backoff=remember_errors.on_error,
                          on_success=remember_errors.on_success,
                          # No jitter as we want a constant value
                          jitter=None
                          )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


class Error(Exception):
    """Base exception for the API interaction module"""


class GraphQLThrottledError(Exception):
    def __init__(self, msg=None, code=None):
        Exception.__init__(self, msg)
        self.code = code

    """Base exception for the API interaction module"""


class GraphQLGeneralError(Exception):
    """Base exception for the API interaction module"""

    def __init__(self, msg=None, code=None):
        Exception.__init__(self, msg)
        self.code = code


class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    key_properties = ['id']
    # Controls which SDK object we use to call the API by default.
    replication_object = None
    # Status parameter override option
    status_key = None

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
        return self.replication_object.find(**query_params)

    def get_objects(self):
        updated_at_min = self.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
        results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)

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

            singer.log_info("getting from %s - %s", updated_at_min,
                            updated_at_max)
            while True:
                status_key = self.status_key or "status"
                query_params = {
                    "since_id": since_id,
                    "created_at_min": updated_at_min,
                    "created_at_max": updated_at_max,
                    "limit": results_per_page,
                    status_key: "any"
                }

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
                if len(objects) < results_per_page:
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

            updated_at_min = updated_at_max + datetime.timedelta(seconds=1)

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj.to_dict()

    def get_children_by_graph_ql(self, child, child_parameters):
        LOGGER.info("Getting data with GraphQL")

        updated_at_min = self.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
        results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)

        # Page through till the end of the resultset
        while updated_at_min < stop_time:
            after = None
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)

            if updated_at_max > stop_time:
                updated_at_max = stop_time
            singer.log_info("getting from %s - %s", updated_at_min,
                            updated_at_max)
            while True:
                query = self.get_graph_query(updated_at_min,
                                             updated_at_max,
                                             150,
                                             child,
                                             child_parameters,
                                             results_per_page,
                                             after=after)
                with metrics.http_request_timer(self.name):
                    data = self.excute_graph_ql(query)

                data = data[self.name]
                page_info = data['pageInfo']
                edges = data["edges"]
                for edge in edges:
                    after = edge["cursor"]
                    node = edge["node"]
                    yield node
                if not page_info["hasNextPage"]:
                    Context.state.get('bookmarks', {}).get(self.name, {}).pop('since_id', None)
                    self.update_bookmark(utils.strftime(updated_at_max + datetime.timedelta(seconds=1)))
                    break

            updated_at_min = updated_at_max + datetime.timedelta(seconds=1)

    @shopify_error_handling
    def excute_graph_ql(self, query):

        try:
            # the execute function sometimes prints and this causes errors for the target, so I block printing for it
            with HiddenPrints():
                response = json.loads(shopify.GraphQL().execute(query))
        except Exception:
            raise GraphQLGeneralError("Execution failed", code=500)

        if 'data' in response:
            return response['data']
        else:
            if "errors" in response:
                errors = response["errors"]
                singer.log_info(errors)
                if errors[0]["extensions"]["code"] == "THROTTLED":
                    raise GraphQLThrottledError("THROTTLED", code=429)

            raise GraphQLGeneralError("Failed", code=500)

    def get_graph_query(self, created_at_min, created_at_max, limit, child, child_parameters, child_limit=100,
                        after=None):
        query = """{
                      orders(first:%i %s ,query:"created_at:>'%s' AND created_at:<'%s'") {
                        pageInfo { # Returns details about the current page of results
                          hasNextPage # Whether there are more results after this page
                          hasPreviousPage # Whether there are more results before this page
                        }
                        edges{
                          cursor
                          node{
                            id,
                            %s(first:%i){
                              %s
                            }
                            createdAt
                          }
                        }
                      }
                }"""
        after_str = ''
        if after:
            after_str = ',after:"%s"' % after
        query = query % (limit, after_str, created_at_min, created_at_max, child, child_limit, child_parameters)
        return query

    def get_table_schema(self):
        streams = Context.catalog["streams"]
        schema = None
        for stream in streams:
            if stream["tap_stream_id"] == self.name:
                schema = stream["schema"]
                break

        return schema

    def get_graph_ql_prop(self, schema):
        properties = schema["properties"]
        ql_fields = []
        for prop in properties:
            if "generated" in properties[prop]["type"]:
                continue
            if 'object' in properties[prop]['type']:
                if properties[prop]["properties"]:
                    ql_field = "%s{%s}" % (prop, self.get_graph_ql_prop(properties[prop]))
                    ql_fields.append(ql_field)
            elif 'array' in properties[prop]['type']:
                if properties[prop]["items"]["properties"]:
                    ql_field = "%s{%s}" % (prop, self.get_graph_ql_prop(properties[prop]["items"]))
                    ql_fields.append(ql_field)
            else:
                ql_fields.append(prop)
        return ','.join(ql_fields)
