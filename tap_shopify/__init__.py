#!/usr/bin/env python3
import os
import datetime
import json
import time
import math
import copy

import pyactiveresource
import shopify
import singer
from singer import utils
from singer import metadata
from singer import Transformer
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError
from tap_shopify.streams.base import shopify_error_handling, get_request_timeout
import tap_shopify.streams # Load stream objects into Context
from tap_shopify.rule_map import RuleMap

REQUIRED_CONFIG_KEYS = ["shop", "api_key"]
LOGGER = singer.get_logger()
SDC_KEYS = {'id': 'integer', 'name': 'string', 'myshopify_domain': 'string'}

@shopify_error_handling
def initialize_shopify_client():
    api_key = Context.config['api_key']
    shop = Context.config['shop']
    version = '2021-04'
    session = shopify.Session(shop, version, api_key)
    shopify.ShopifyResource.activate_session(session)

    # set request timeout
    shopify.Shop.set_timeout(get_request_timeout())

    # Shop.current() makes a call for shop details with provided shop and api_key
    return shopify.Shop.current().attributes

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


def get_discovery_metadata(stream, schema, rule_map, stream_name):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    if 'stream_name' in rule_map:
        # Write original-name of stream name in top level metadata
        mdata = metadata.write(mdata, (), 'original-name', stream_name)

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        # Add metadata for nested(child) fields also if it's name is changed from original name.
        add_child_into_metadata(schema['properties'][field_name], metadata, mdata,
                                    rule_map, ('properties', field_name), )
        if ('properties', field_name) in rule_map:
            mdata.get(('properties', field_name)).update(
                {'original-name': rule_map[('properties', field_name)]})

    return metadata.to_list(mdata)

def add_child_into_metadata(schema, m_data, mdata, rule_map, parent=()):
    """
    Add metadata for nested(child) fields also if it's name is changed from original name.
    """
    if schema and isinstance(schema, dict) and schema.get('properties'):
        for key in schema['properties'].keys():
            # prepare key to find original-name of field in rule_map object
            # Key is tuple of items found in breadcrumb.
            breadcrumb = parent + ('properties', key)

            # Iterate in recursive manner to go through each field of schema.
            add_child_into_metadata(schema['properties'][key], m_data, mdata, rule_map, breadcrumb)

            if breadcrumb in rule_map:
                # Update field name as standard name in breadcrumb if it is found in rule_map.
                mdata = m_data.write(mdata, breadcrumb, 'inclusion', 'available')

                # Add `original-name` field in metadata which contain actual name of field.
                mdata.get(breadcrumb).update({'original-name': rule_map[breadcrumb]})

    if schema.get('anyOf'):
        for schema_fields in schema.get('anyOf'):
            add_child_into_metadata(schema_fields, m_data, mdata, rule_map, parent)

    if schema and isinstance(schema, dict) and schema.get('items'):
        breadcrumb = parent + ('items',)
        add_child_into_metadata(schema['items'], m_data, mdata, rule_map, breadcrumb)

def load_schema_references():
    shared_schema_file = "definitions.json"
    shared_schema_path = get_abs_path('schemas/')

    refs = {}
    with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
        refs[shared_schema_file] = json.load(data_file)

    return refs

def add_synthetic_key_to_schema(schema):
    for k in SDC_KEYS:
        schema['properties']['_sdc_shop_' + k] = {'type': ["null", SDC_KEYS[k]]}
    return schema

def discover(rule_map):
    initialize_shopify_client() # Checking token in discover mode

    raw_schemas = load_schemas()
    streams = []

    refs = load_schema_references()
    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # resolve_schema_references() is changing value of passed refs.
        # Customer is a stream and it's a nested field of orders and abandoned_checkouts streams
        # and those 3 _sdc fields are also added inside nested field customer for above 2 stream
        # so create a copy of refs before passing it to resolve_schema_references().
        refs_copy = copy.deepcopy(refs)
        catalog_schema = add_synthetic_key_to_schema(
            singer.resolve_schema_references(schema, refs_copy))

        # Define stream_name in GetStdFieldsFromApiFields
        rule_map.GetStdFieldsFromApiFields[schema_name] = {}

        # Get updated schema by applying rule map
        standard_catalog_schema = rule_map.apply_ruleset_on_schema(catalog_schema, schema_name)

        # Get standard name of schema
        standard_schema_name = rule_map.apply_rule_set_on_stream_name(schema_name)

        # create and add catalog entry
        catalog_entry = {
            'stream': standard_schema_name,
            'tap_stream_id': standard_schema_name,
            'schema': standard_catalog_schema,
            'metadata': get_discovery_metadata(stream, schema,
                            rule_map.GetStdFieldsFromApiFields[schema_name], schema_name),
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

# pylint: disable=too-many-locals
def sync(rule_map):
    shop_attributes = initialize_shopify_client()
    sdc_fields = {"_sdc_shop_" + x: shop_attributes[x] for x in SDC_KEYS}

    temp_sdc_fields = {}
    for sdc_field in sdc_fields.keys():
        temp_sdc_fields[sdc_field] = rule_map.apply_rules_to_original_field(sdc_field)

    for temp_sdc_field, temp_sdc_field_value in temp_sdc_fields.items():
        sdc_fields[temp_sdc_field_value] = sdc_fields.pop(temp_sdc_field)

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

        # Fill rule_map object by original-name available in metadata
        rule_map.fill_rule_map_object_by_catalog(stream_id,
                                    metadata.to_map(catalog_entry['metadata']))

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

                # Apply rule map on record
                rec = rule_map.apply_ruleset_on_api_response(rec, stream_id)

                rec = transformer.transform({**rec, **sdc_fields},
                                            record_schema,
                                            record_metadata)
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
    try:
        # Parse command line arguments
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        Context.config = args.config
        Context.state = args.state

        rule_map = RuleMap()

        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            catalog = discover(rule_map)
            print(json.dumps(catalog, indent=2))
        # Otherwise run in sync mode
        else:
            Context.tap_start = utils.now()
            if args.catalog:
                Context.catalog = args.catalog.to_dict()
            else:
                Context.catalog = discover(rule_map)

            sync(rule_map)
    except pyactiveresource.connection.ResourceNotFound as exc:
        raise ShopifyError(exc, 'Ensure shop is entered correctly') from exc
    except pyactiveresource.connection.UnauthorizedAccess as exc:
        raise ShopifyError(exc, 'Invalid access token - Re-authorize the connection') \
            from exc
    except pyactiveresource.connection.ConnectionError as exc:
        msg = ''
        try:
            body_json = exc.response.body.decode()
            body = json.loads(body_json)
            msg = body.get('errors')
        finally:
            raise ShopifyError(exc, msg) from exc
    except Exception as exc:
        raise ShopifyError(exc) from exc

if __name__ == "__main__":
    main()
