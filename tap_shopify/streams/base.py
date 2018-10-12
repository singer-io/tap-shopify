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
    replication_object = None
    key_properties = None

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, obj):
        # Assuming that ordering works for bookmarking
        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            self.replication_key,
            # All bookmarkable streams boomark `updated_at`
            obj.updated_at)
        singer.write_state(Context.state)

    def get_objects(self, parent_object=None):
        start_date=self.get_bookmark()
        page = 1
        while True:
            count = 0

            try:
                if parent_object:
                    # FIXME definitely not paginating or setting
                    # reasonable limit
                    objects = parent_object.metafields()
                else:
                    objects = self.replication_object.find(
                        # Max allowed value as of 2018-09-19 11:53:48
                        limit=RESULTS_PER_PAGE,
                        # TODO do we need `status='any'` here or something? See abandoned_checkouts
                        page=page,
                        updated_at_min=start_date,
                        # Order is an undocumented query param that we believe
                        # ensures the order of the results.
                        order="updated_at asc")
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
            for obj in objects:
                self.update_bookmark(obj)
                yield obj
                count += 1

            LOGGER.info('%s Count = %s', self.name, count)

            if len(objects) < RESULTS_PER_PAGE:
                break
            page += 1
