import shopify

from tap_shopify.streams.base import Stream
from tap_shopify.context import Context

class Locations(Stream):
    name = 'locations'
    replication_object = shopify.Location
    replication_key = None
    replication_method = "FULL_TABLE"

    def get_locations_data(self):
        location_page = self.replication_object.find()
        yield from location_page

        while location_page.has_next_page():
            location_page = location_page.next_page()
            yield from location_page

    def get_objects(self):
        # get all locations data as it is used for child streams
        # if we get locations updated after a date
        # then there is possibility of data loss for child streams
        for obj in self.get_locations_data():
            yield obj

Context.stream_objects['locations'] = Locations
