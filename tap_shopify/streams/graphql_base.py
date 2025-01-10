import datetime
import functools
import socket
from urllib.error import URLError
import http
import backoff
import requests
import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 100  # GraphQL typically uses cursor-based pagination
REQUEST_TIMEOUT = 300
DATE_WINDOW_SIZE = 1
MAX_RETRIES = 5

class ShopifyGraphQLError(Exception):
    """Custom exception for GraphQL errors"""
    pass

def get_request_timeout():
    request_timeout = REQUEST_TIMEOUT
    timeout_from_config = Context.config.get('request_timeout')
    if timeout_from_config and float(timeout_from_config):
        request_timeout = float(timeout_from_config)
    return request_timeout

def shopify_graphql_error_handling(fnc):
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=MAX_RETRIES,
        factor=2
    )
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=MAX_RETRIES,
        giveup=lambda e: 400 <= e.response.status_code < 500,
        factor=2
    )
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        giveup=lambda e: e.response.status_code != 429,
        on_backoff=lambda details: LOGGER.info(
            "Rate limited -- sleeping for %s seconds",
            details['wait']
        ),
        jitter=None
    )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

class GraphQLStream:
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'updatedAt'
    key_properties = ['id']
    
    def __init__(self):
        self.request_timeout = get_request_timeout()
        self.results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)
        self.shop_url = f"https://{Context.config['shop']}.myshopify.com/admin/api/2025-01/graphql.json"
        self.headers = {
            'X-Shopify-Access-Token': Context.config['api_key'],
            'Content-Type': 'application/json',
        }

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state, self.name, self.replication_key)
                   or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, bookmark_value, bookmark_key=None):
        singer.write_bookmark(
            Context.state,
            self.name,
            bookmark_key or self.replication_key,
            bookmark_value
        )
        singer.write_state(Context.state)

    @shopify_graphql_error_handling
    def query(self, query, variables=None):
        response = requests.post(
            self.shop_url,
            headers=self.headers,
            json={'query': query, 'variables': variables},
            timeout=self.request_timeout
        )
        response.raise_for_status()
        result = response.json()
        
        if 'errors' in result:
            raise ShopifyGraphQLError(result['errors'])
            
        return result['data']

    def process_node(self, node):
        """Override this method in the stream implementations to process each node"""
        return node

    def get_objects(self):
        stop_time = singer.utils.now().replace(microsecond=0)
        updated_at_min = self.get_bookmark()
        max_bookmark = updated_at_min
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        while updated_at_min < stop_time:
            updated_at_max = min(
                updated_at_min + datetime.timedelta(days=date_window_size),
                stop_time
            )
            
            variables = {
                'first': self.results_per_page,
                'after': None,
                'query': f"updated_at:>={updated_at_min.isoformat()} AND updated_at:<={updated_at_max.isoformat()}"
            }

            has_next_page = True
            while has_next_page:
                data = self.query(self.get_graphql_query(), variables)
                connection = self.get_connection_from_data(data)
                
                for edge in connection['edges']:
                    node = self.process_node(edge['node'])
                    replication_value = strptime_to_utc(node[self.replication_key])
                    if replication_value > max_bookmark:
                        max_bookmark = replication_value
                    yield node

                page_info = connection['pageInfo']
                has_next_page = page_info['hasNextPage']
                variables['after'] = page_info['endCursor'] if has_next_page else None

            updated_at_min = updated_at_max
            self.update_bookmark(utils.strftime(max_bookmark))

    def get_graphql_query(self):
        """Override this method in the stream implementations to provide the GraphQL query"""
        raise NotImplementedError("Streams must implement get_graphql_query()")

    def get_connection_from_data(self, data):
        """Override this method in the stream implementations to extract the connection object"""
        raise NotImplementedError("Streams must implement get_connection_from_data()")

    def sync(self):
        """Sync data from the Shopify GraphQL API"""
        for obj in self.get_objects():
            yield obj
