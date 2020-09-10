"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
from datetime import datetime as dt

from dateutil.parser import parse

from tap_tester import menagerie, runner
from base import BaseTapTest


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    @staticmethod
    def name():
        return "tap_tester_shopify_bookmark_test"

    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is > the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that only data for incremental streams is sent to the target

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = self.create_connection()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # Our test data sets for Shopify do not have any abandoned_checkouts
        untested_streams = self.child_streams().union({'abandoned_checkouts', 'collects', 'metafields', 'transactions', 'order_refunds'})
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()),
                         incremental_streams.difference(untested_streams))

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):

                # get bookmark values from state and target data
                stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(
                    stream_bookmark_key) == 1  # There shouldn't be a compound replication key
                stream_bookmark_key = stream_bookmark_key.pop()

                state_value = first_sync_state.get("bookmarks", {}).get(
                    stream, {None: None}).get(stream_bookmark_key)
                target_value = first_max_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                target_min_value = first_min_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)

                try:
                    # attempt to parse the bookmark as a date
                    if state_value:
                        if isinstance(state_value, str):
                            state_value = self.local_to_utc(parse(state_value))
                        if isinstance(state_value, int):
                            state_value = self.local_to_utc(dt.utcfromtimestamp(state_value))

                    if target_value:
                        if isinstance(target_value, str):
                            target_value = self.local_to_utc(parse(target_value))
                        if isinstance(target_value, int):
                            target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                    if target_min_value:
                        if isinstance(target_min_value, str):
                            target_min_value = self.local_to_utc(parse(target_min_value))
                        if isinstance(target_min_value, int):
                            target_min_value = self.local_to_utc(
                                dt.utcfromtimestamp(target_min_value))

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                # verify that there is data with different bookmark values - setup necessary
                self.assertGreaterEqual(target_value, target_min_value,
                                        msg="Data isn't set up to be able to test bookmarks")

                # verify state agrees with target data after 1st sync
                self.assertGreaterEqual(state_value, target_value,
                                 msg="The bookmark value isn't correct based on target data")

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second syc didn't have less records, bookmark usage not verified")

                # verify all data from 2nd sync >= 1st bookmark
                target_value = second_min_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                try:
                    if target_value:
                        if isinstance(target_value, str):
                            target_value = self.local_to_utc(parse(target_value))
                        if isinstance(target_value, int):
                            target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")
