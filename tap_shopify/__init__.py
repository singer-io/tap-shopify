#!/usr/bin/env python3
import os
import json
import singer
import shopify
from singer import utils
from singer import metadata
from singer import Transformer

REQUIRED_CONFIG_KEYS = ["api_key"]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

class Orders:
    name = None
    replication_method = None
    replication_key = None
    key_properties = None
    metadata = None
    schema = None

    def __init__(self, schema):
        self.name = "orders"
        self.replication_method = "INCREMENTAL"
        self.replication_key = 'updated_at'
        self.key_properties = ['id']
        self.schema = schema
        self.metadata = self.load_metadata()

    def sync(self, state):
        for order in shopify.Order.find(limit=250):
            yield order.to_dict()

    def load_metadata(self):
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in self.schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


STREAMS = {
    'orders': Orders
}


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        stream = STREAMS[schema_name](schema)

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = stream.metadata
        stream_key_properties = stream.key_properties

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : stream.metadata,
            'key_properties': stream.key_properties
        }
        streams.append(catalog_entry)

    return {'streams': streams}

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
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def initialize_shopify_client(config):
    api_key = config['api_key']
    shop = config['shop']
    session = shopify.Session("%s.myshopify.com" % (shop),
                              api_key)
    shopify.ShopifyResource.activate_session(session)

def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for catalog_entry in catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']
        initialize_shopify_client(config)
        stream = STREAMS[stream_id](stream_schema)

        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream:' + stream_id)

            # write schema message
            singer.write_schema(stream.name, stream.schema, stream.key_properties)

            # sync
            with Transformer() as transformer:
                for rec in stream.sync(state):
                    rec = transformer.transform(rec, stream.schema, metadata.to_map(stream.metadata))
                    singer.write_record(stream.name, rec)

    return

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
            catalog =  discover()

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
