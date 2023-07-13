import shopify
from singer import utils
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context

class Locations(Stream):
    name = 'locations'
    replication_object = shopify.Location
    # Added decorator over functions of shopify SDK
    replication_object.find = shopify_error_handling(replication_object.find)

    def get_locations_data(self):
        # set timeout
        self.replication_object.set_timeout(self.request_timeout)
        location_page = self.replication_object.find()
        yield from location_page

        while location_page.has_next_page():
            location_page = location_page.next_page()
            yield from location_page

    def sync(self):
        for location in self.get_locations_data():
            yield location.to_dict()

Context.stream_objects['locations'] = Locations
