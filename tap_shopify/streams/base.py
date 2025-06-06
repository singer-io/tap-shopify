from datetime import timedelta
import functools
import json
import re
import socket
import urllib
from urllib.error import URLError
import http
import backoff
import pyactiveresource
import pyactiveresource.formats
import shopify
import simplejson
import singer
from singer import metrics, utils
from graphql import parse, print_ast, visit
from graphql.language import Visitor, FieldNode, SelectionSetNode, OperationDefinitionNode, NameNode
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

DEFAULT_DATE_WINDOW = 30

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5


# function to return request timeout
def get_request_timeout():

    request_timeout = REQUEST_TIMEOUT # set default timeout
    timeout_from_config = Context.config.get('request_timeout')
    # updated the timeout value if timeout is passed in config and not from 0, "0", ""
    if timeout_from_config and float(timeout_from_config):
        # update the request timeout for the requests
        request_timeout = float(timeout_from_config)

    return request_timeout

def execute_gql(self, query, variables=None, operation_name=None, timeout=None):
    """
    This overrides the `execute` method from ShopifyAPI(v12.6.0) to remove the print statement
    and also to explicitly pass the timeout value to the urlopen method.
    Ensure to check the original impl before making any changes or upgrading the SDK version,
    as this modification may affect future updates
    """
    default_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    headers = self.merge_headers(default_headers, self.headers)
    data = {"query": query, "variables": variables, "operationName": operation_name}

    req = urllib.request.Request(self.endpoint, json.dumps(data).encode("utf-8"), headers)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as http_error:
        raise http_error

shopify.GraphQL.execute  = execute_gql

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
    @backoff.on_exception(backoff.expo,
                          (http.client.IncompleteRead, ConnectionResetError,
                           ShopifyAPIError, ShopifyError),
                          max_tries=MAX_RETRIES,
                          factor=2)
    @backoff.on_exception(backoff.expo, # timeout error raise by Shopify
                          (pyactiveresource.connection.Error, socket.timeout),
                          giveup=is_timeout_error,
                          max_tries=MAX_RETRIES,
                          factor=2)
    @backoff.on_exception(backoff.expo,
                          (pyactiveresource.connection.ServerError,
                           pyactiveresource.formats.Error,
                           simplejson.scanner.JSONDecodeError,
                           URLError),
                          giveup=is_not_status_code_fn(range(500, 599)),
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(backoff.expo,
                          pyactiveresource.connection.ResourceNotFound,
                          giveup=is_not_status_code_fn([404]),
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(backoff.expo,
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

class ShopifyAPIError(Error):
    """Raised for any unexpected api error without a valid status code"""

class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'updatedAt'
    key_properties = ['id']
    date_window_size = None
    data_key = None
    results_per_page = None

    def __init__(self):
        self.results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)
        self.date_window_size = float(Context.config.get("date_window_size") or
                                      DEFAULT_DATE_WINDOW) or DEFAULT_DATE_WINDOW

        # set request timeout
        self.request_timeout = get_request_timeout()

    def get_query(self):
        """
        Provides GraphQL query
        """
        raise NotImplementedError("Function Not Implemented")

    def transform_object(self, obj):
        """
        Modify this to perform custom transformation on each object
        """
        return obj

    @classmethod
    def camel_to_snake(cls, name):
        """
        Convert camelCase to snake_case

        Args:
            name (str): Input string in camelCase

        Returns:
            str: Converted string in snake_case
        """
        # Handle special cases
        if not name:
            return name

        # Use regex to insert underscore before capital letters
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        return pattern.sub('_', name).lower()

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def get_bookmark_by_name(self, bookmark_key):
        # name is overridden by some substreams
        bookmark = (singer.get_bookmark(Context.state,
                                        self.name,
                                        bookmark_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)


    def get_since_id(self):
        return singer.get_bookmark(Context.state,
                                   # name is overridden by some substreams
                                   self.name,
                                   'since_id')

    def get_updated_at_max(self):
        updated_at_max = Context.state.get('bookmarks', {}).get(self.name, {}).get('updated_at_max')
        return utils.strptime_with_tz(updated_at_max) if updated_at_max else None

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

    def remove_fields_from_query(self, fields_to_remove: list) -> str:
        ast = parse(self.get_query())
        used_variable_names = set()

        class FieldRemover(Visitor):
            def enter_selection_set(self, node, _key, _parent, _path, _ancestors):
                new_selections = []
                for selection in node.selections:
                    if isinstance(selection, FieldNode):
                        if selection.name.value in fields_to_remove:
                            continue
                        # Check field arguments for variable usage
                        for arg in selection.arguments or []:
                            if hasattr(arg.value, "name") and isinstance(arg.value.name, NameNode):
                                used_variable_names.add(arg.value.name.value)
                    new_selections.append(selection)
                return SelectionSetNode(selections=new_selections)

            def leave_operation_definition(self, node, *_):
                # Keep only variable definitions that are used
                new_var_defs = [
                    var_def for var_def in node.variable_definitions or []
                    if var_def.variable.name.value in used_variable_names
                ]
                return OperationDefinitionNode(
                    operation=node.operation,
                    name=node.name,
                    variable_definitions=new_var_defs,
                    directives=node.directives,
                    selection_set=node.selection_set
                )

        # Start visiting the AST and dynamically gather used variable names
        modified_ast = visit(ast, FieldRemover())
        return print_ast(modified_ast)

    # This function can be overridden by subclasses for specialized API
    # interactions. If you override it you need to remember to decorate it
    # with shopify_error_handling to get 429 and 500 handling.
    # pylint: disable=E1123
    @shopify_error_handling
    def call_api(self, query_params, query=None, data_key=None):
        """
        - Modifies the default call API implementation to support GraphQL
        - Returns response Object dict
        """
        try:
            query = query or self.get_query()
            data_key = data_key or self.data_key
            LOGGER.info("Fetching %s %s", self.name, query_params)
            response = shopify.GraphQL().execute(
                query=query,
                variables=query_params,
                timeout=self.request_timeout
            )
            response = json.loads(response)
            if "errors" in response.keys():
                raise ShopifyAPIError(response["errors"])

            data = response.get("data", {}).get(data_key, {})
            return data

        except ShopifyAPIError as gql_error:
            LOGGER.error("GraphQL Error: %s", gql_error)
            raise ShopifyAPIError("An error occurred with the GraphQL API.") from gql_error

        except urllib.error.HTTPError as http_error:
            # Extract X-Request-ID from the error response headers
            request_id = http_error.headers.get("X-Request-ID")
            error_body = http_error.read().decode("utf-8") if http_error.fp else None
            error_message = (
                f"{http_error.reason} - {error_body}"
                if error_body
                else http_error.reason
            )
            raise ShopifyError(http_error,
                f"GraphQL request failed for stream '{self.name}' with status {http_error.code} "
                f"and X-Request-ID '{request_id or 'N/A'}', Reason: {error_message}."
            ) from http_error

        except Exception as exc:
            LOGGER.error("Unexpected error occurred.")
            raise exc

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        rkey = self.camel_to_snake(self.replication_key)
        params = {
            "query": f"{rkey}:>='{updated_at_min}' AND {rkey}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }

        if cursor:
            params["after"] = cursor
        return params

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Pagination & Filtering of stream
            - Transformation and bookmarking
        """

        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)
            has_next_page, cursor = True, None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)

                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params, query=query)

                for edge in data.get("edges"):
                    obj = self.transform_object(edge.get("node"))
                    replication_value = utils.strptime_to_utc(obj[self.replication_key])
                    current_bookmark = max(current_bookmark, replication_value)
                    yield obj

                page_info =  data.get("pageInfo")
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            last_updated_at = query_end
            # Update bookmark to the latest value, but not beyond sync start time
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

    def sync(self):
        """
        Default implementation for sync method
        """
        yield from self.get_objects()
