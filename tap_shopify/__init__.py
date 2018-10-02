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
    session = shopify.Session("%s.myshopify.com" % (shop),
                              api_key)
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
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            raw_dict = json.load(file)
            schema = singer.resolve_schema_references(raw_dict, raw_dict)
            schemas[file_raw] = schema

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


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : get_discovery_metadata(stream, schema),
            'key_properties': stream.key_properties,
            'replication_key': stream.replication_key,
            'replication_method': stream.replication_method
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync():
    initialize_shopify_client()

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
        stream = Context.stream_objects[stream_id]()

        if (Context.streams.get(stream_id) and (Context.is_selected(stream_id) or
                                                Context.has_selected_child(stream_id))):
            LOGGER.info('Syncing stream: %s', stream_id)

            for (tap_stream_id, rec) in stream.sync():
                with Transformer() as transformer:
                    extraction_time = singer.utils.now()
                    record_stream = Context.get_catalog_entry(tap_stream_id)
                    record_schema = record_stream['schema']
                    record_metadata = metadata.to_map(record_stream['metadata'])
                    rec = transformer.transform(rec, record_schema, record_metadata)
                    singer.write_record(tap_stream_id,
                                        rec,
                                        time_extracted=extraction_time)


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
