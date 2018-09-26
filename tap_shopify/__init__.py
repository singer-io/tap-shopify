#!/usr/bin/env python3
import os
import json
import time
import math

import singer
from singer import utils
from singer import metadata
from singer import Transformer
import pyactiveresource
import shopify

REQUIRED_CONFIG_KEYS = ["api_key"]
LOGGER = singer.get_logger()
RESULTS_PER_PAGE = 250

def initialize_shopify_client(config):
    api_key = config['api_key']
    shop = config['shop']
    session = shopify.Session("%s.myshopify.com" % (shop),
                              api_key)
    activate_resp = shopify.ShopifyResource.activate_session(session)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    # This schema represents many of the currency values as JSON schema
    # 'number's, which may result in lost precision.
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            raw_dict = json.load(file)
            schema = singer.resolve_schema_references(raw_dict, raw_dict)
            schemas[file_raw] = schema

    return schemas


def get_discovery_metadata(stream):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in stream.schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        stream = STREAMS[schema_name](schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : get_discovery_metadata(stream),
            'key_properties': stream.key_properties
        }
        streams.append(catalog_entry)

    return {'streams': streams}

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = None
    schema = None

    def __init__(self, schema):
        self.schema = schema

    def get_bookmark(self, state, config):
        bookmark = singer.get_bookmark(state, self.name, self.replication_key) or config["start_date"]
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, state, config, value):
        current_bookmark = self.get_bookmark(state, config)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)

    def paginate_endpoint(self, state, config, call_endpoint, start_date):
        page = 1
        while True:
            try:
                values = call_endpoint(page, start_date)
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
                    LOGGER.ERROR("Received a {} error.".format(resp.code))
                    raise
            for value in values:
                dict_value = value.to_dict()
                self.update_bookmark(state, config, dict_value[self.replication_key])
                yield dict_value

            if not isinstance(self, SubStream):
                singer.write_state(state)

            if len(values) < RESULTS_PER_PAGE:
                break
            page += 1

class Orders(Stream):
    name = 'orders'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']

    def call_endpoint(self, page, start_date):
        return shopify.Order.find(
            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            page=page,
            updated_at_min=start_date,
            # Order is an undocumented query param that we believe
            # ensures the order of the results.
            order="updated_at asc")

    def sync(self, config, state):
        start_date = self.get_bookmark(state, config)
        count = 0

        for order in self.paginate_endpoint(state, config, self.call_endpoint, start_date):
            yield order
            count += 1

        LOGGER.info('Count = {}'.format(count))

class SubStream(Stream):
    """
    A SubStream may optionally have a parent, if so, it adapts its bookmarking to access
    either at the root level, or beneath the parent key, if specified.

    A SubStream must follow these principles:
    1. It needs its own bookmark field.
    2. If parent records don't get updated when child records are updated, it needs a lookback
       window tuned to the expected activity window of the data.
       - The parent will check for child updates on its records within this window.
    3. Child records should only be synced up to the start time of the sync run, in case
       they get updated during the tap's run time.
    4. To solve for selecting the child stream later than the parent, the parent sync needs
       to start requesting data from the min(parent, child, start_date) bookmark
    5. Mark the initial bookmark for either stream as the `start_date` of the config so
       that we don't emit records outside of the requested range
    6. Write state only after a guaranteed "full sync"
       - If the parent is queried using a sliding time window, write child bookmarks, but
         don't use them until the full window is finished.
    """
    parent_type = None
    parent_lookback_window = 0

    def __init__(self, schema, parent_type=None):
        self.parent_type = parent_type
        super().__init__(schema)

    def get_bookmark(self, state, config):
        if self.parent_type is None:
            bookmark = singer.get_bookmark(state, self.name, self.replication_key) or config["start_date"]
        else:
            bookmark = singer.get_bookmark(state, self.parent_type, self.name) or config["start_date"]
            if isinstance(bookmark, dict):
                bookmark = bookmark.get(self.replication_key) or config["start_date"]
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, state, config, value):
        current_bookmark = self.get_bookmark(state, config)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            if self.parent_type is None:
                singer.write_bookmark(state, self.name, self.replication_key, value)
            else:
                root_bookmarks = state.get("bookmarks")
                if root_bookmarks is None:
                    state["bookmarks"] = {}
                parent_bookmark = state.get("bookmarks", {}).get(self.parent_type)
                if parent_bookmark is None:
                    state["bookmarks"][self.parent_type] = {}
                child_bookmark = singer.get_bookmark(state, self.parent_type, self.name) or {}
                child_bookmark[self.replication_key] = value
                singer.write_bookmark(state, self.parent_type, self.name, child_bookmark)

class Metafields(SubStream):
    name = 'metafields'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']

    def _call_root_endpoint(self, page, start_date):
        shopify.Metafield.find(
            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            page=page,
            updated_at_min=start_date,
            # Order is an undocumented query param that we believe
            # ensures the order of the results.
            order="updated_at asc")

    def _sync_root(self, config, state):
        start_date = self.get_bookmark(state, config)
        count = 0

        metafields = self.paginate_endpoint(state, config, self._call_root_endpoint, start_date)
        for metafield in metafields:
            yield metafield
            count += 1

        LOGGER.info('Count = {}'.format(count))

    def sync(self, config, state, parent_id=None):
        # Two Options: Child and Root
        # If Root
        if self.parent_type is None:
            self._sync_root(config, state)
        else:
            # Sync child metafields for parent ID
            pass

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams


STREAMS = {
    'orders': Orders
}


def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for catalog_entry in catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']
        stream = STREAMS[stream_id](stream_schema)
        stream_metadata = metadata.to_map(catalog_entry['metadata'])

        initialize_shopify_client(config)


        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: %s', stream_id)

            # write schema message
            singer.write_schema(stream.name, stream.schema, stream.key_properties)

            # sync
            with Transformer() as transformer:
                for rec in stream.sync(config, state):
                    rec = transformer.transform(rec, stream.schema, stream_metadata)
                    singer.write_record(stream.name, rec)


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:

        # 'properties' is the legacy name of the catalog
        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
