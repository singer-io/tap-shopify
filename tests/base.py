"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import datetime as dt
from datetime import timezone as tz
import dateutil.parser
import pytz
from datetime import timedelta
from tap_tester import connections, menagerie, runner


class BaseTapTest(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00+00:00"
    DEFAULT_RESULTS_PER_PAGE = 175

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-shopify"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.shopify"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2017-07-01T00:00:00Z',
            'shop': 'stitchdatawearhouse',
            'date_window_size': 30,
            # BUG: https://jira.talendforge.org/browse/TDL-13180
            # 'results_per_page': '50'
        }

        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return_value['shop'] = 'talenddatawearhouse'
        return return_value

    @staticmethod
    def get_credentials(original_credentials: bool = True):
        """Authentication information for the test account"""

        if original_credentials:
            return {
                'api_key': os.getenv('TAP_SHOPIFY_API_KEY_STITCHDATAWEARHOUSE')
            }

        return {
            'api_key': os.getenv('TAP_SHOPIFY_API_KEY_TALENDDATAWEARHOUSE')
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: self.DEFAULT_RESULTS_PER_PAGE}

        meta = default.copy()
        meta.update({self.FOREIGN_KEYS: {"owner_id", "owner_resource"}})

        return {
            "abandoned_checkouts": {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                # BUG: https://jira.talendforge.org/browse/TDL-13180
                self.API_LIMIT: 50},
            "collects": default,
            "custom_collections": default,
            "smart_collections": default,
            "custom_collections_products": default,
            "smart_collections_products": default,
            "customers": default,
            "orders": default,
            "order_refunds": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: self.DEFAULT_RESULTS_PER_PAGE},
            "products": default,
            "inventory_items": {self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 250},
            "metafields": meta,
            "transactions": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.FOREIGN_KEYS: {"order_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: self.DEFAULT_RESULTS_PER_PAGE},
            "locations": {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 0},
            "inventory_levels": {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"location_id", "inventory_item_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: self.DEFAULT_RESULTS_PER_PAGE},
            "events": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 50
            }
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.FOREIGN_KEYS)}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {table: properties.get(self.FOREIGN_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x
                        for x in [os.getenv('TAP_SHOPIFY_API_KEY_STITCHDATAWEARHOUSE'),
                                  os.getenv('TAP_SHOPIFY_API_KEY_TALENDDATAWEARHOUSE')]
                        if x is None]
        if missing_envs:
            raise Exception("set environment variables")

    #########################
    #   Helper Methods      #
    #########################

    def create_connection(self, original_properties: bool = True, original_credentials: bool = True):
        """Create a new connection with the test name"""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties, original_credentials)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        return sync_record_count

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc"""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute,
                 date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        """Return the minimum value for the replication key for each stream"""
        min_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            (stream_bookmark_key, ) = stream_bookmark_key

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get("start_date")
        self.store_1_streams = {'custom_collections', 'orders', 'products', 'customers', 'locations', 'inventory_levels', 'inventory_items', 'events'}
        self.store_2_streams = {'abandoned_checkouts', 'collects', 'metafields', 'transactions', 'order_refunds', 'products', 'locations', 'inventory_levels', 'inventory_items', 'events'}

    #modified this method to accommodate replication key in the current_state
    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [0,5,0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}

        for stream, state in current_state['bookmarks'].items():
            # modified state value to accommodate the replication key
            stream = stream
            state_key, state_value = list(state.items())[0]

            state_as_datetime = dateutil.parser.parse(list(state.values())[0])

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = "%Y-%m-%dT00:00:00Z"
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)
            state = {state_key: calculated_state_formatted}
            stream_to_calculated_state[stream] = state

        return stream_to_calculated_state

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))
