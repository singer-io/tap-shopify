import time
import datetime
import math

import pyactiveresource
import singer
from singer import utils
from tap_shopify.context import Context

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 250

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = None

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state, self.name, self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def query_start(self):
        min_bookmark = self.get_bookmark()
        for sub_stream_name in Context.streams.get(self.name, []):
            if not Context.is_selected(sub_stream_name):
                continue
            sub_stream = Context.stream_objects[sub_stream_name](parent_type=self.name)
            sub_stream_bookmark = sub_stream.get_bookmark()
            look_back = sub_stream.parent_lookback_window
            adjusted_sub_bookmark = sub_stream_bookmark - datetime.timedelta(days=look_back)
            if adjusted_sub_bookmark < min_bookmark:
                min_bookmark = adjusted_sub_bookmark
        return min_bookmark

    def update_bookmark(self, value):
        current_bookmark = self.get_bookmark()
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(Context.state, self.name, self.replication_key, value)

    def sync_substreams(self, parent_obj, start_bookmark):
        sub_stream_names = Context.streams.get(self.name, [])
        for sub_stream_name in sub_stream_names:
            if Context.is_selected(sub_stream_name):
                sub_stream = Context.stream_objects[sub_stream_name](parent_type=self.name)
                values = sub_stream.sync(parent_obj, start_bookmark)
                for value in values:
                    yield value

    def paginate_endpoint(self, call_endpoint, start_date):
        page = 1
        while True:
            try:
                values = call_endpoint(page, start_date)
            except pyactiveresource.connection.ClientError as client_error:
                # We have never seen this be anything _but_ a 429. Other
                # states should be consider untested.
                resp = client_error.response
                if resp.code == 429:
                    # Retry-After is an undocumented header. But honoring
                    # it was proven to work in our spikes.
                    sleep_time_str = resp.headers['Retry-After']
                    LOGGER.info("Received 429 -- sleeping for %s seconds", sleep_time_str)
                    time.sleep(math.floor(float(sleep_time_str)))
                    continue
                else:
                    LOGGER.ERROR("Received a %s error.", resp.code)
                    raise
            for value in values:
                # Only update the bookmark if we are actually syncing this stream's records
                # Applicable when a parent is being requested to retrieve child records
                bookmark_value = getattr(value, self.replication_key)
                bookmark_datetime = utils.strptime_with_tz(bookmark_value)

                if Context.is_selected(self.name) and bookmark_datetime < Context.tap_start:
                    self.update_bookmark(bookmark_value)
                yield value

            singer.write_state(Context.state)

            if len(values) < RESULTS_PER_PAGE:
                break
            page += 1



class SubStream(Stream):
    """
    A SubStream may optionally have a parent, if so, it adapts its bookmarking to access
    either at the root level, or beneath the parent key, if specified.

    A SubStream must follow these principles:
    1. It needs its own bookmark field.
    2. If parent records don't get updated when child records are updated, it needs a lookback
       window tuned to the expected activity window of the data.
       - The parent will check for child updates on its records within this window.
    3. Child records should only be synced up to the start time of the sync run, in case
       they get updated during the tap's run time.
    4. To solve for selecting the child stream later than the parent, the parent sync needs
       to start requesting data from the min(parent, child, start_date) bookmark, adjusted
       for lookback window
    5. Treat the initial bookmark for either stream as the `start_date` of the config so
       that we don't emit records outside of the requested range
    6. Write state only after a guaranteed "full sync"
       - If the parent is queried using a sliding time window, write child bookmarks, but
         don't use them until the full window is finished.
    """
    parent_type = None
    parent_lookback_window = 0

    def __init__(self, parent_type=None):
        self.parent_type = parent_type

    def get_bookmark(self):
        if self.parent_type is None:
            bookmark = (singer.get_bookmark(Context.state, self.name, self.replication_key)
                        or Context.config["start_date"])
        else:
            bookmark = (singer.get_bookmark(Context.state, self.parent_type, self.name)
                        or Context.config["start_date"])
            if isinstance(bookmark, dict):
                bookmark = bookmark.get(self.replication_key) or Context.config["start_date"]
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, value):
        current_bookmark = self.get_bookmark()
        if value and utils.strptime_with_tz(value) > current_bookmark:
            if self.parent_type is None:
                singer.write_bookmark(Context.state, self.name, self.replication_key, value)
            else:
                root_bookmarks = Context.state.get("bookmarks")
                if root_bookmarks is None:
                    Context.state["bookmarks"] = {}
                parent_bookmark = Context.state.get("bookmarks", {}).get(self.parent_type)
                if parent_bookmark is None:
                    Context.state["bookmarks"][self.parent_type] = {}
                child_bookmark = (singer.get_bookmark(Context.state, self.parent_type, self.name)
                                  or {})
                child_bookmark[self.replication_key] = value
                singer.write_bookmark(Context.state, self.parent_type, self.name, child_bookmark)
