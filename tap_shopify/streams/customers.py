import datetime
import shopify
import singer
import re
import requests

from singer import metrics, utils
from tap_shopify.streams.base import Stream
from tap_shopify.context import Context

LOGGER = singer.get_logger()

class Customers(Stream):
    name = 'customers'
    replication_object = shopify.Customer

    def call_api(self, query_params):
        url = "https://{}.myshopify.com/admin/api/2019-10/customers.json".format(Context.config['shop'])
        headers = {'X-Shopify-Access-Token': Context.config['api_key']}
        LOGGER.info("Calling url: %s", url)
        resp = requests.get(url, params=query_params, headers=headers)
        return resp


    def get_next_page(self, link):
        headers = {'X-Shopify-Access-Token': Context.config['api_key']}
        LOGGER.info("Calling url: %s", link)
        resp = requests.get(link, headers=headers)
        return resp


    def get_objects(self):
        updated_at_min = self.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", 1))
        results_per_page = int(Context.config.get("results_per_page", 250))

        while updated_at_min < stop_time:
            # Create query params for the window size
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            query_params = {
                "updated_at_min": updated_at_min,
                "updated_at_max": updated_at_max,
                "limit": results_per_page,
                "status": "any"
            }

            # Get the first page outside of the inner loop
            with metrics.http_request_timer(self.name):
                resp = self.call_api(query_params)

            while True:
                # Parse the response json and yield the customer objects
                # for obj in resp.json():
                #     yield obj

                # Get the next page via the Link header and get a new response
                url = resp.links.get('next', {}).get('url')
                if not url:
                    LOGGER.info("Done paginating customers.")
                    self.update_bookmark(utils.strftime(updated_at_max))
                    break

                # Retrieve the next page for the next loop
                resp = self.get_next_page(url)

            updated_at_min = updated_at_max


Context.stream_objects['customers'] = Customers
