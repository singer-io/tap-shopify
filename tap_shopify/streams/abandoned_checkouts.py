import datetime
import singer
from singer import metrics, utils
import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()

DATE_WINDOW_SIZE = 1

class AbandonedCheckouts(Stream):
    name = 'abandoned_checkouts'
    replication_object = shopify.Checkout

    def get_objects(self):
        updated_at_min = self.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Page through till the end of the resultset
        while updated_at_min < stop_time:

            # It's important that `updated_at_min` has microseconds
            # truncated. Why has been lost to the mists of time but we
            # think it has something to do with how the API treats
            # microseconds on its date windows. Maybe it's possible to
            # drop data due to rounding errors or something like that?
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time

            status_key = self.status_key or "status"
            query_params = self.get_query_params(None,
                                                 status_key,
                                                 updated_at_min,
                                                 updated_at_max)
            query_params.pop("since_id")

            while True:
                with metrics.http_request_timer(self.name):
                    objects = self.call_api(query_params)
                    LOGGER.info("Made request %s", objects._metadata['headers']['X-Request-ID'])

                for obj in objects:
                    yield obj

                if objects.has_next_page():
                    next_page_url = objects.next_page_url
                    query_params = {"from_":next_page_url}
                else:
                    break

            self.update_bookmark(utils.strftime(updated_at_max))
            updated_at_min = updated_at_max


Context.stream_objects['abandoned_checkouts'] = AbandonedCheckouts
