import shopify
from singer import utils
from tap_shopify.streams.base import Stream
from tap_shopify.context import Context

class Locations(Stream):
    name = 'locations'
    replication_object = shopify.Location

    def get_locations_data(self):
        location_page = self.replication_object.find()
        yield from location_page

        while location_page.has_next_page():
            location_page = location_page.next_page()
            yield from location_page

    def get_objects(self):
        bookmark = self.get_bookmark()
        max_bookmark = utils.strftime(utils.now())

        for obj in self.get_locations_data():
            if utils.strptime_with_tz(obj.updated_at) > bookmark:
                yield obj
        self.update_bookmark(max_bookmark)

Context.stream_objects['locations'] = Locations
