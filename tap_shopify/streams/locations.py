import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Location(Stream):
    name = 'locations'
    replication_object = shopify.Location
    key_properties = ['id']
    replication_method = 'FULL_TABLE'

    def get_objects(self):
        return self.call_api({})


Context.stream_objects['locations'] = Location
