#!/usr/bin/env python3
import os
import datetime
import json
import time
import math

import pyactiveresource
import shopify
import singer
from singer import utils
from singer import metadata
from singer import Transformer
from tap_shopify.context import Context
import tap_shopify.streams # Load stream objects into Context

REQUIRED_CONFIG_KEYS = ["shop", "api_key"]
LOGGER = singer.get_logger()

def initialize_shopify_client():
    api_key = Context.config['api_key']
    shop = Context.config['shop']
    session = shopify.Session(shop, api_key)
    shopify.ShopifyResource.activate_session(session)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    # This schema represents many of the currency values as JSON schema
    # 'number's, which may result in lost precision.
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        schema_name = filename.replace('.json', '')
        with open(path) as file:
            schemas[schema_name] = json.load(file)

    return schemas


def get_discovery_metadata(stream, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)

def load_schema_references():
    shared_schema_file = "definitions.json"
    shared_schema_path = get_abs_path('schemas/')

    refs = {}
    with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
        refs[shared_schema_file] = json.load(data_file)

    return refs

def discover():
    raw_schemas = load_schemas()
    streams = []

    refs = load_schema_references()
    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': singer.resolve_schema_references(schema, refs),
            'metadata' : get_discovery_metadata(stream, schema),
            'key_properties': stream.key_properties,
            'replication_key': stream.replication_key,
            'replication_method': stream.replication_method
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def shuffle_streams(stream_name):
    '''
    Takes the name of the first stream to sync and reshuffles the order
    of the list to put it at the top
    '''
    matching_index = 0
    for i, catalog_entry in enumerate(Context.catalog["streams"]):
        if catalog_entry["tap_stream_id"] == stream_name:
            matching_index = i
    top_half = Context.catalog["streams"][matching_index:]
    bottom_half = Context.catalog["streams"][:matching_index]
    Context.catalog["streams"] = top_half + bottom_half

def sync():
    initialize_shopify_client()

    # Emit all schemas first so we have them for child streams
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            singer.write_schema(stream["tap_stream_id"],
                                stream["schema"],
                                stream["key_properties"],
                                bookmark_properties=stream["replication_key"])
            Context.counts[stream["tap_stream_id"]] = 0

    # If there is a currently syncing stream bookmark, shuffle the
    # stream order so it gets sync'd first
    currently_sync_stream_name = Context.state.get('bookmarks', {}).get('currently_sync_stream')
    if currently_sync_stream_name:
        shuffle_streams(currently_sync_stream_name)

    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream = Context.stream_objects[stream_id]()

        if not Context.is_selected(stream_id):
            LOGGER.info('Skipping stream: %s', stream_id)
            continue

        LOGGER.info('Syncing stream: %s', stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}
        Context.state['bookmarks']['currently_sync_stream'] = stream_id

        with Transformer() as transformer:
            for rec in stream.sync():
                extraction_time = singer.utils.now()
                record_schema = catalog_entry['schema']
                record_metadata = metadata.to_map(catalog_entry['metadata'])
                rec = transformer.transform(rec, record_schema, record_metadata)
                singer.write_record(stream_id,
                                    rec,
                                    time_extracted=extraction_time)
                Context.counts[stream_id] += 1

        Context.state['bookmarks'].pop('currently_sync_stream')
        singer.write_state(Context.state)

    LOGGER.info('----------------------')
    for stream_id, stream_count in Context.counts.items():
        LOGGER.info('%s: %d', stream_id, stream_count)
    LOGGER.info('----------------------')

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
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.config = args.config
        Context.state = args.state
        sync()

if __name__ == "__main__":
    main()
