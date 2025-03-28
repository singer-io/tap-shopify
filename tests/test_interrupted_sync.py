"""
Test tap successfully resumes after interrupted sync without missing any records
"""
import random

from datetime import datetime as dt

from tap_tester import menagerie, connections, runner, LOGGER
from base import BaseTapTest


class InterruptedSyncTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    @staticmethod
    def name():
        return "tap_tester_shopify_int_sync_test"

    def group_streams(self, sync_order, currently_syncing):
        self.assertIn(currently_syncing, sync_order,
                      msg="Currently sycning stream not found in sync order")
        index = len(sync_order)
        for i, stream in enumerate(sync_order):
            if stream == currently_syncing:
                index = i
                break
        return {
            "completed": sync_order[:index],
            "yet_to_be_synced": sync_order[(index + 1):],
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2022-03-01T00:00:00Z'

    def test_run(self):

        conn_id = self.create_connection(original_properties=False, original_credentials=False)

        expected_streams = {'customers',
                            'collections',
                            'orders',
                            'products',
                            'transactions'}

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Our test data sets for Shopify do not have any abandoned_checkouts
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in expected_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        #################################
        # Run first sync
        #################################

        first_sync_record_count = self.run_sync(conn_id)
        first_sync_state = menagerie.get_state(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_order = runner.get_stream_sync_order_from_target()

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertSetEqual(set(first_sync_record_count.keys()), expected_streams)

        # BUG:TDL-17087 : State has additional values which are not streams
        # Need to remove additional values from bookmark value
        extra_stuff = {'transaction_orders',
                       'metafield_products',
                       'refund_orders',
                       'product_variants'}

        for keys in list(first_sync_state['bookmarks'].keys()):
            if keys in extra_stuff:
                first_sync_state['bookmarks'].pop(keys)

        ################################
        # Update State between Syncs
        ################################

        # hardcoding the updated state to ensure atleast 1 record in resuming (2nd) sync.
        # values have been provided after reviewing the max bookmark value for each of the streams
        currently_syncing_stream = random.choice(list(expected_streams))
        LOGGER.info("Randomly selected currently syncing stream: %s", currently_syncing_stream)

        stream_groups = self.group_streams(first_sync_order, currently_syncing_stream)
        completed_streams = stream_groups.get('completed')
        yet_to_be_synced_streams = stream_groups.get('yet_to_be_synced')

        base_state = {'bookmarks':
                     {'currently_sync_stream': currently_syncing_stream,
                      'customers': first_sync_state.get('bookmarks').get('customers'),
                      'orders': first_sync_state.get('bookmarks').get('orders'),
                      'products': first_sync_state.get('bookmarks').get('products'),
                      'transactions': first_sync_state.get('bookmarks').get('transactions')
                      }}

        # remove yet to be synced streams from base state and then set new state
        new_state = {
            'bookmarks': {
                key: val
                for key, val in base_state['bookmarks'].items()
                if key not in yet_to_be_synced_streams
            }
        }

        menagerie.set_state(conn_id, new_state)

        ################################
        # Run Resuming (2nd) Sync
        ################################

        resuming_sync_record_count = self.run_sync(conn_id)
        resuming_sync_records = runner.get_records_from_target_output()
        resuming_sync_state = menagerie.get_state(conn_id)
        resuming_sync_order = runner.get_stream_sync_order_from_target()

        LOGGER.info("First sync stream order: %s", first_sync_order)
        LOGGER.info("currently syncing stream: %s", currently_syncing_stream)
        LOGGER.info("yet to be synced streams: %s", yet_to_be_synced_streams)
        LOGGER.info("completed streams: %s", completed_streams)
        LOGGER.info("Resuming sync stream order: %s", resuming_sync_order)

        # tap level assertions
        self.assertTrue(first_sync_state.get('bookmarks'))
        self.assertTrue(resuming_sync_state.get('bookmarks'))
        self.assertIsNone(first_sync_state.get('bookmarks', {}).get('currently_sync_stream'))
        self.assertIsNone(resuming_sync_state.get('bookmarks', {}).get('currently_sync_stream'))

        # verify streams are shuffled so the resuming sync starts with currently_syncing_stream
        self.assertEqual(resuming_sync_order[0], currently_syncing_stream)

        expected_resuming_sync_order = (
            [currently_syncing_stream] + yet_to_be_synced_streams + completed_streams
        )
        self.assertListEqual(expected_resuming_sync_order, resuming_sync_order)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values (rep method = incremental for all shopify streams as of Jul-2023)
                expected_replication_keys = self.expected_replication_keys()
                # information required for assertions from sync 1 and 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                resuming_sync_count = resuming_sync_record_count.get(stream, 0)

                first_sync_messages = [
                    record.get('data') for record
                    in first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                resuming_sync_messages = [
                    record.get('data') for record
                    in resuming_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                replication_key = next(iter(expected_replication_keys[stream]))
                first_bookmark_stream = first_sync_state.get('bookmarks', {}).get(stream, {})
                first_bookmark_value = first_bookmark_stream.get(replication_key)
                resuming_bookmark_stream = resuming_sync_state.get('bookmarks', {}).get(stream, {})
                resuming_bookmark_value = resuming_bookmark_stream.get(replication_key)
                resuming_bookmark_value_utc = self.convert_state_to_utc(resuming_bookmark_value)

                if stream in new_state['bookmarks'].keys():
                    simulated_bookmark = new_state['bookmarks'][stream]
                    simulated_bookmark_value = simulated_bookmark[replication_key]

                youngest_first_sync_date = max(
                    self.parse_date(record.get(replication_key))
                    for record in first_sync_messages)

                # verify the syncs sets a bookmark of the expected form
                self.assertIsNotNone(first_bookmark_value)
                self.assertTrue(self.is_expected_date_format(first_bookmark_value))
                self.assertIsNotNone(resuming_bookmark_value)
                self.assertTrue(self.is_expected_date_format(resuming_bookmark_value))

                # verify the resuming bookmark is greater or equal than 1st sync bookmark
                self.assertGreaterEqual(resuming_bookmark_value, first_bookmark_value)

                # verify oldest record from resuming sync respects bookmark from previous sync
                if stream in new_state['bookmarks'].keys() and resuming_sync_messages:
                    # if metafields owner_resource != 'shop' resuming_sync_messages can be empty
                    actual_oldest_resuming_replication_date = min(
                        self.parse_date(record.get(replication_key))
                        for record in resuming_sync_messages)

                    self.assertEqual(actual_oldest_resuming_replication_date,
                                     self.parse_date(simulated_bookmark_value),
                                     msg="Oldest resuming sync record not respecting bookmark")

                    # all interrupted recs are in full recs, interrupted rec counts verified
                    first_sync_records_after_bookmark = [
                        record for record in first_sync_messages
                        if self.parse_date(record[replication_key]) >=
                        self.parse_date(simulated_bookmark_value)]
                    # remove any records that got added after the first sync
                    filtered_resuming_records = [
                        record for record in resuming_sync_messages
                        if self.parse_date(record[replication_key]) <=
                        youngest_first_sync_date]

                    self.assertEqual(first_sync_records_after_bookmark, filtered_resuming_records,
                                     msg="Incorrect data in the resuming sync")

                for record in resuming_sync_messages:
                    replication_key_value = record.get(replication_key)
                    # this assertion is only for completed and interrupted streams
                    if stream in new_state['bookmarks'].keys():
                        # verify 2nd sync rep key value is greater or equal to 1st sync bookmarks
                        msg = "Resuming sync records do not respect the previous bookmark"
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg=msg)
                    # verify the 2nd sync bookmark value is the max rep key value for given stream
                    msg = ("Resuming sync bookmark was set incorrectly, a record with a greater"
                           " replication key value was synced")
                    self.assertLessEqual(replication_key_value, resuming_bookmark_value_utc,
                                         msg=msg)

                # verify less data in 2nd sync for streams that started or completed
                if stream in new_state['bookmarks'].keys():
                    self.assertLess(resuming_sync_count, first_sync_count,
                                    msg="Resuming sync record count greater than expected")

                # verify yet to be sync'd streams have equal oldest record
                if stream in yet_to_be_synced_streams:
                    oldest_first_sync_replication_date = min(
                        self.parse_date(record.get(replication_key))
                        for record in first_sync_messages)
                    oldest_resuming_sync_replication_date = min(
                        self.parse_date(record.get(replication_key))
                        for record in resuming_sync_messages)
                    self.assertEqual(oldest_resuming_sync_replication_date,
                                     oldest_first_sync_replication_date)

                # verify that we get at least 1 record in the resuming sync
                self.assertGreater(resuming_sync_count, 0, msg="Resuming sync yielded 0 recs")
