import shopify
from singer import utils
from tap_shopify.streams.base import (Stream, shopify_error_handling)
from tap_shopify.context import Context

class Shop(Stream):
    name = 'shop'
    replication_object = shopify.Shop

    @shopify_error_handling
    def api_call_for_shop_data(self):
        return self.replication_object.current()

    def get_shop_data(self):
        shop_page = [self.api_call_for_shop_data()]
        yield from shop_page

    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark

        for shop in self.get_shop_data():

            shop_dict = shop.to_dict()
            replication_value = utils.strptime_to_utc(shop_dict[self.replication_key])

            if replication_value >= bookmark:
                yield shop_dict

            # update max bookmark if "replication_value" of current shop is greater
            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(utils.strftime(max_bookmark))

Context.stream_objects['shop'] = Shop
