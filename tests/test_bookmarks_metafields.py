"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
from datetime import datetime as dt

from dateutil.parser import parse

from tap_tester import menagerie, runner, LOGGER
from base import BaseTapTest

class BookmarkMetafieldsTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    @staticmethod
    def name():
        return "tap_tester_shopify_bookmark_metafields_test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2025-01-01T00:00:00Z'

    def min_max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        min_bookmarks = {}
        metafields_order = []
        metafields_product = []
        metafields_customer = []
        metafields_collection = []
        for stream, batch in sync_records.items():
            for message in batch.get('messages'):
                if message['action'] == 'upsert':
                    if message.get('data').get('ownerType') == "ORDER":
                        metafields_order.append(message.get('data'))
                    elif message.get('data').get('ownerType') == "PRODUCT":
                        metafields_product.append(message.get('data'))
                    elif message.get('data').get('ownerType') == "CUSTOMER":
                        metafields_customer.append(message.get('data'))
                    elif message.get('data').get('ownerType') == "COLLECTION":
                        metafields_collection.append(message.get('data'))
            
        stream_bookmark_key = self.expected_replication_keys().get(stream, set())
        assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
        stream_bookmark_key = stream_bookmark_key.pop()

        for owner_type, messages in [("metafields_order", metafields_order), ("metafields_product", metafields_product), ("metafields_customer", metafields_customer), ("metafields_collection", metafields_collection)]:
            max_bookmarks, min_bookmarks = self.fetch_max_min(owner_type, stream_bookmark_key, messages, max_bookmarks, min_bookmarks)
        
        max_bookmarks["metafields"] = max_bookmarks
        min_bookmarks["metafields"] = min_bookmarks

        return max_bookmarks, min_bookmarks


    def fetch_max_min(self, owner_type, stream_bookmark_key, messages, max_bookmarks, min_bookmarks):
        bk_values = [message.get(stream_bookmark_key) for message in messages]
        max_bookmarks[owner_type] = None
        min_bookmarks[owner_type] = None
        for bk_value in bk_values:
            if bk_value is None:
                continue

            if max_bookmarks[owner_type] is None:
                max_bookmarks[owner_type] = bk_value

            if bk_value > max_bookmarks[owner_type]:
                max_bookmarks[owner_type] = bk_value

            if min_bookmarks[owner_type] is None:
                min_bookmarks[owner_type] = bk_value

            if bk_value < min_bookmarks[owner_type]:
                min_bookmarks[owner_type] = bk_value
        return max_bookmarks, min_bookmarks

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
        conn_id = self.create_connection(original_credentials=True)

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_stream = {'metafields'}

        # Our test data sets for Shopify do not have any abandoned_checkouts
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_stream]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()),
                         incremental_stream)

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()

        first_max_bookmarks, first_min_bookmarks = self.min_max_bookmarks_by_stream(first_sync_records)
        #first_sync_bookmarks = menagerie.get_state(conn_id)
        
        #######################
        # Update State between Syncs
        #######################

        # new_state = {'bookmarks': dict()}
        # simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)

        # for stream, updated_state in simulated_states.items():
        #     new_state['bookmarks'][stream] = updated_state
        # menagerie.set_state(conn_id, new_state)

        _ , second_min_bookmarks = self.min_max_bookmarks_by_stream(second_sync_records)

        for stream in incremental_stream:
            with self.subTest(stream=stream):

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second sync didn't have less records, bookmark usage not verified")

                # get bookmark values from state and target data
                stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(
                    stream_bookmark_key) == 1  # There shouldn't be a compound replication key
                stream_bookmark_key = stream_bookmark_key.pop()

                for metafield_key in {"metafields_order", "metafields_product", "metafields_customer", "metafields_collection"}:

                    state_value = first_sync_state.get("bookmarks", {}).get(
                        stream, {None: None}).get(metafield_key)
                    target_value = first_max_bookmarks.get(
                        stream, {None: None}).get(metafield_key)
                    target_min_value = first_min_bookmarks.get(
                        stream, {None: None}).get(metafield_key)

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
                        LOGGER.warn("bookmarks cannot be converted to dates, comparing values directly")

                    # verify that there is data with different bookmark values - setup necessary
                    self.assertGreaterEqual(target_value, target_min_value,
                                            msg="Data isn't set up to be able to test bookmarks")

                    # verify state agrees with target data after 1st sync
                    self.assertGreaterEqual(state_value, target_value,
                                    msg="The bookmark value isn't correct based on target data")

                    # verify all data from 2nd sync >= 1st bookmark
                    target_value = second_min_bookmarks.get(
                        stream, {None: None}).get(metafield_key)
                    try:
                        if target_value:
                            if isinstance(target_value, str):
                                target_value = self.local_to_utc(parse(target_value))
                            if isinstance(target_value, int):
                                target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                    except (OverflowError, ValueError, TypeError):
                        LOGGER.warn("bookmarks cannot be converted to dates, comparing values directly")

                    # TODO: Loop over and verify all records from 2nd sync >= 1st bookmark
                    for message in second_sync_records.get(stream).get('messages'):
                        if message['action'] == 'upsert':
                            if message.get('data').get('ownerType') == metafield_key:
                                self.assertGreaterEqual(
                                    message.get('data').get(stream_bookmark_key), target_value,
                                    msg="Data from 2nd sync is not all >= bookmark from 1st sync")
