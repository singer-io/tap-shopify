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

REQUIRED_CONFIG_KEYS = ["shop", "api_key"]
LOGGER = singer.get_logger()
RESULTS_PER_PAGE = 250

class Context():
    config = None
    state = {}
    catalog = None

    @classmethod
    def is_selected(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            return True
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if entry['breadcrumb'] == () and entry['metadata'].get('selected', None):
                    return True

        return False

    @classmethod
    def has_selected_child(cls, stream_name):
        for sub_stream_name in SUB_STREAMS.get(stream_name, []):
            if cls.is_selected(sub_stream_name):
                return True
        return False

    @classmethod
    def get_schema(cls, stream_name):
        stream =  [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

def initialize_shopify_client():
    api_key = Context.config['api_key']
    shop = Context.config['shop']
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
            'key_properties': stream.key_properties,
            'replication_key': stream.replication_key,
            'replication_method': stream.replication_method
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

    def get_bookmark(self):
        bookmark = singer.get_bookmark(Context.state, self.name, self.replication_key) or Context.config["start_date"]
        return utils.strptime_with_tz(bookmark)

    def get_min_bookmark(self):
        min_bookmark = self.get_bookmark()
        for sub_stream_name in SUB_STREAMS.get(self.name, []):
            if not Context.is_selected(sub_stream_name):
                continue
            sub_stream = STREAMS[sub_stream_name](Context.get_schema(sub_stream_name), parent_type=self.name)
            sub_stream_bookmark = sub_stream.get_bookmark()
            if sub_stream_bookmark < min_bookmark:
                min_bookmark = sub_stream_bookmark
        return min_bookmark

    def update_bookmark(self, value):
        current_bookmark = self.get_bookmark()
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(Context.state, self.name, self.replication_key, value)

    def paginate_endpoint(self, call_endpoint, start_date):
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
                # Only update the bookmark if we are actually syncing this stream's records
                # Applicable when a parent is being requested to retrieve child records
                if Context.is_selected(self.name):
                    self.update_bookmark(getattr(value, self.replication_key))
                yield value

            if not isinstance(self, SubStream):
                singer.write_state(Context.state)

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

    def sync(self):
        orders_bookmark = self.get_bookmark()
        start_bookmark = self.get_min_bookmark()
        count = 0

        for order in self.paginate_endpoint(self.call_endpoint, start_bookmark):
            updated_at = utils.strptime_with_tz(order.updated_at)
            if (Context.is_selected(self.name) and
                updated_at >= orders_bookmark):
                count += 1
                yield (self.name, order.to_dict())

            sub_stream_names = SUB_STREAMS.get(self.name, [])
            for sub_stream_name in sub_stream_names:
                if Context.is_selected(sub_stream_name):
                    sub_stream = STREAMS[sub_stream_name](Context.get_schema(sub_stream_name), parent_type=self.name)
                    values = sub_stream.sync(order)
                    for value in values:
                        yield value

        LOGGER.info('Orders Count = {}'.format(count))

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

    def get_bookmark(self):
        if self.parent_type is None:
            bookmark = singer.get_bookmark(Context.state, self.name, self.replication_key) or Context.config["start_date"]
        else:
            bookmark = singer.get_bookmark(Context.state, self.parent_type, self.name) or Context.config["start_date"]
            if isinstance(bookmark, dict):
                bookmark = bookmark.get(self.replication_key) or Context.config["start_date"]
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, value):
        current_bookmark = self.get_bookmark()
        if value and utils.strptime_with_tz(value) > current_bookmark:
            if self.parent_type is None:
                singer.write_bookmark(Context.state, self.name, self.replication_key, value)
            else:
                root_bookmarks = Context.state.get("bookmarks")
                if root_bookmarks is None:
                    Context.state["bookmarks"] = {}
                parent_bookmark = Context.state.get("bookmarks", {}).get(self.parent_type)
                if parent_bookmark is None:
                    Context.state["bookmarks"][self.parent_type] = {}
                child_bookmark = singer.get_bookmark(Context.state, self.parent_type, self.name) or {}
                child_bookmark[self.replication_key] = value
                singer.write_bookmark(Context.state, self.parent_type, self.name, child_bookmark)

class Metafields(SubStream):
    name = 'metafields'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']

    def _call_root_endpoint(self, page, start_date):
        return shopify.Metafield.find(
            # Max allowed value as of 2018-09-19 11:53:48
            limit=RESULTS_PER_PAGE,
            page=page,
            updated_at_min=start_date,
            # Order is an undocumented query param that we believe
            # ensures the order of the results.
            order="updated_at asc")

    def _sync_root(self):
        start_date = self.get_bookmark()
        count = 0

        metafields = self.paginate_endpoint(self._call_root_endpoint, start_date)
        for metafield in metafields:
            yield (self.name, metafield.to_dict())
            count += 1

        LOGGER.info('Shop Metafields Count = {}'.format(count))

    def _sync_child(self, parent_obj):
        start_date = self.get_bookmark()
        count = 0

        def call_child_endpoint(page, start_date):
            return shopify.Metafield.find(
                # Max allowed value as of 2018-09-19 11:53:48
                limit=RESULTS_PER_PAGE,
                page=page,
                updated_at_min=start_date,
                # Order is an undocumented query param that we believe
                # ensures the order of the results.
                order="updated_at asc",
                **{"metafield[owner_id]": parent_obj.id,
                   "metafield[owner_resource]": self.parent_type})

        metafields = self.paginate_endpoint(call_child_endpoint, start_date)
        for metafield in metafields:
            yield (self.name, metafield.to_dict())
            count += 1

        LOGGER.info('{} Metafields Count = {}'.format(self.parent_type.replace('_', ' ').title(),
                                                      count))

    def sync(self, parent_obj=None):
        if self.parent_type is None:
            for rec in self._sync_root():
                yield rec
        else:
            for rec in self._sync_child(parent_obj):
                yield rec

STREAMS = {
    'orders': Orders,
    'metafields': Metafields
}

SUB_STREAMS = {
    'orders': ['metafields']
}


def sync():

    # Emit all schemas first so we have them for child streams
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            singer.write_schema(stream["tap_stream_id"],
                                stream["schema"],
                                stream["key_properties"],
                                bookmark_properties=stream["replication_key"])

    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']
        stream = STREAMS[stream_id](stream_schema)
        stream_metadata = metadata.to_map(catalog_entry['metadata'])

        initialize_shopify_client()

        if Context.is_selected(stream_id) or Context.has_selected_child(stream_id):
            LOGGER.info('Syncing stream: %s', stream_id)

            with Transformer() as transformer:
                for (tap_stream_id, rec) in stream.sync():
                    extraction_time = singer.utils.now()
                    rec = transformer.transform(rec, stream.schema, stream_metadata)
                    singer.write_record(tap_stream_id, rec, time_extracted=extraction_time)


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
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.config = args.config
        Context.state = args.state
        sync()

if __name__ == "__main__":
    main()
