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
        bookmark = self.get_bookmark()
        max_bookmark = bookmark

        for location in self.get_locations_data():

            location_dict = location.to_dict()
            replication_value = utils.strptime_to_utc(location_dict[self.replication_key])

            if replication_value >= bookmark:
                yield location_dict

            # update max bookmark if "replication_value" of current location is greater
            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(utils.strftime(max_bookmark))

Context.stream_objects['locations'] = Locations
