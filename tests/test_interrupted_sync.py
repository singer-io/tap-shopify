"""
Test tap successfully resumes after interrupted sync without missing any records
"""
from datetime import datetime as dt

from tap_tester import menagerie, connections, runner
from base import BaseTapTest


class InterruptedSyncTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    @staticmethod
    def name():
        return "tap_tester_shopify_int_sync_test"

    # function for verifying the date format
    def is_expected_date_format(self, date):
        try:
            # parse date
            dt.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            # return False if date is in not expected format
            return False
        # return True in case of no error
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2022-03-01T00:00:00Z'

    # BHT re-eval below comment TODO
    # We are currently working with 1 store as we do not have access to stitchdatawarehouse store
    # def test_run_store_1(self):
    #     with self.subTest(store="store_1"):
    #         conn_id = self.create_connection(original_credentials=True)
    #         self.bookmarks_test(conn_id, self.store_1_streams)

    # new global only for this test
    global interrupt_test_streams
    interrupt_test_streams = {'collects',
                              'customers',
                              'events',
                              'metafields',
                              'orders',
                              'products',
                              'transactions'}

    def test_run_store_2(self):
        with self.subTest(store="store_2"):
            conn_id = self.create_connection(original_properties=False, original_credentials=False)
            self.interrupted_sync_test(conn_id, interrupt_test_streams)

    def interrupted_sync_test(self, conn_id, testable_streams):

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL and key in testable_streams}

        # Our test data sets for Shopify do not have any abandoned_checkouts
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        #################################
        # Run first sync
        #################################

        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), incremental_streams)

        first_sync_state = menagerie.get_state(conn_id)
        first_sync_records = runner.get_records_from_target_output()
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

        #new_state = {'bookmarks': {'currently_sync_stream': 'transactions'}}
        new_state = {'bookmarks': {'currently_sync_stream': 'customers'}}
        currently_syncing_stream = new_state['bookmarks']['currently_sync_stream']
        completed_streams = {
            'collects', 'customers', 'events', 'transactions'} - set(currently_syncing_stream)
        yet_to_be_synced_streams = {'metafields', 'orders', 'products'}

        # hardcoding the updated state to ensure atleast 1 record in resuming (2nd) sync.
        # values have been provided after reviewing the max bookmark value for each of the streams
        simulated_states = {'collects': {'updated_at': '2023-01-01T09:08:28.000000Z'},
                            'customers': {'updated_at': '2023-03-28T18:53:28.000000Z'},
                            'events': {'created_at': '2023-01-22T05:05:53.000000Z'},
                            #'metafields': {'updated_at': '2023-01-07T21:18:05.000000Z'},
                            #'orders': {'updated_at': '2023-01-20T05:09:01.000000Z'},
                            #'products': {'updated_at': '2023-01-20T05:10:05.000000Z'},
                            'transactions': {'created_at': '2022-06-26T00:06:38-04:00'}
                            }

        for stream, updated_state in simulated_states.items():
            new_state['bookmarks'][stream] = updated_state
        menagerie.set_state(conn_id, new_state)

        ################################
        # Run Resuming (2nd) Sync
        ################################

        resuming_sync_record_count = self.run_sync(conn_id)
        resuming_sync_records = runner.get_records_from_target_output()
        resuming_sync_state = menagerie.get_state(conn_id)
        resuming_sync_order = runner.get_stream_sync_order_from_target()

        # tap level assertions
        self.assertIsNone(first_sync_state.get('bookmarks').get('currently_sync_stream'))
        self.assertIsNone(resuming_sync_state.get('bookmarks').get('currently_sync_stream'))

        # TODO why is metafields stream always sync'd first?
        self.assertEqual(resuming_sync_order[0], 'metafields')

        # verify resuming sync starts with currently syncing stream
        self.assertEqual(resuming_sync_order[1], currently_syncing_stream)

        # verify resuming sync continues with yet to be sync'd streams
        actual_next_synced = set(resuming_sync_order[2:1 + len(yet_to_be_synced_streams)])
        modified_yet_to_be_synced = yet_to_be_synced_streams.difference({'metafields'})
        self.assertSetEqual(actual_next_synced, modified_yet_to_be_synced)

        # verify resuming sync finishes with completed streams, remove 'collects' stream if present
        if 'collects' in resuming_sync_order:
            actual_last_synced = set(
                resuming_sync_order[-len(completed_streams):]).difference({'collects'})
        else:
            actual_last_synced = set(resuming_sync_order[-(len(completed_streams)-1):])

        modified_completed_streams = completed_streams.difference({'collects'})
        self.assertSetEqual(actual_last_synced, modified_completed_streams)

        for stream in testable_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = self.expected_replication_method()
                expected_replication_keys = self.expected_replication_keys()
                # information required for assertions from sync 1 and 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                resuming_sync_count = resuming_sync_record_count.get(stream, 0)
                """
                The metafields fetches the fields from `products`, `customers`, `orders` and
                `custom_collections` if the parent streams are selected along with the `shop`
                fields.  These different streams have their own bookmark based on the parent.
                Hence filtered out the main records i.e. the `shop` records from all the records.
                """
                if stream != 'metafields':
                    first_sync_messages = [
                        record.get('data') for record
                        in first_sync_records.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert']
                else:
                    first_sync_messages = [
                        record.get('data') for record
                        in first_sync_records.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert'
                        and record.get('data').get('owner_resource') == 'shop']

                if stream != 'metafields':
                    resuming_sync_messages = [
                        record.get('data') for record
                        in resuming_sync_records.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert']
                else:
                    resuming_sync_messages = [
                        record.get('data') for record
                        in resuming_sync_records.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert'
                        and record.get('data').get('owner_resource') == 'shop']

                first_bookmark_value = first_sync_state.get('bookmarks', {}).get(stream, {})
                first_bookmark_value = list(first_bookmark_value.values())[0]
                resuming_bookmark_value = resuming_sync_state.get('bookmarks', {}).get(stream, {})
                resuming_bookmark_value = list(resuming_bookmark_value.values())[0]

                replication_key = next(iter(expected_replication_keys[stream]))
                first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                resuming_bookmark_value_utc = self.convert_state_to_utc(resuming_bookmark_value)
                if stream in simulated_states.keys():
                    simulated_bookmark = new_state['bookmarks'][stream]
                else:
                    simulated_bookmark = None
                if stream in simulated_states.keys():
                    simulated_bookmark_value = list(simulated_bookmark.values())[0]
                else:
                    simulated_bookmark_value = None

                youngest_first_sync_date = max(
                    self.parse_date(record.get(replication_key))
                    for record in first_sync_messages)

                if len(resuming_sync_messages) > 0:  # 'collects' stream may be empty
                    actual_oldest_resuming_replication_date = min(
                        self.parse_date(record.get(replication_key))
                        for record in resuming_sync_messages)

                if stream in simulated_states.keys():
                    expected_resuming_sync_start_time = self.parse_date(first_bookmark_value)
                else:
                    # For not yet started streams there will be no bookmark and we should
                    # sync all records the from the beginning of the original sync.
                    expected_resuming_sync_start_time = min(
                        self.parse_date(record.get(replication_key))
                        for record in first_sync_messages)

                # verify the syncs sets a bookmark of the expected form
                self.assertIsNotNone(first_bookmark_value)
                self.assertTrue(self.is_expected_date_format(first_bookmark_value))
                self.assertIsNotNone(resuming_bookmark_value)
                self.assertTrue(self.is_expected_date_format(resuming_bookmark_value))

                # verify the resuming bookmark is greater than 1st sync bookmark
                """
                This is the expected behaviour for shopify as they are using date windowing
                TDL-17096 : Resuming bookmark value is getting assigned from execution time rather
                than the actual bookmark time for some streams.
                TODO this is not the case for the transactions stream, they are equal.
                """
                self.assertGreaterEqual(resuming_bookmark_value, first_bookmark_value)

                # verify oldest record from resuming sync respects bookmark from previous sync
                if resuming_sync_messages and stream in simulated_states.keys():
                    self.assertEqual(actual_oldest_resuming_replication_date,
                                     self.parse_date(simulated_bookmark_value))

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

                    self.assertEqual(first_sync_records_after_bookmark,
                                     filtered_resuming_records,
                                     msg="Incorrect data in the resuming sync")

                for record in resuming_sync_messages:
                    replication_key_value = record.get(replication_key)
                    # this assertion is only for completed and interrupted streams
                    if stream in simulated_states.keys():
                        # verify 2nd sync rep key value is greater or equal to 1st sync bookmarks
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg=("Resuming sync records do not respect the"
                                                     " previous bookmark")
                                                )
                    # verify the 2nd sync bookmark value is the max rep key value for given stream
                    self.assertLessEqual(replication_key_value, resuming_bookmark_value_utc,
                                         msg=("Resuming sync bookmark was set incorrectly, a record"
                                              " with a greater replication key value was synced")
                                         )

                # verify that we get less data in the 2nd sync
                # all records in the collects stream have the same replication key value, so this
                # assertion does not apply to the collects stream
                if stream in simulated_states.keys() and stream not in ('collects'):
                    self.assertLess(resuming_sync_count, first_sync_count,
                                    msg="Resuming sync record count greater than expected")

                # verify yet to be sync'd streams have equal oldest record
                if stream in yet_to_be_synced_streams:
                    oldest_first_sync_replication_date = min(
                        self.parse_date(record[replication_key])
                        for record in first_sync_messages)
                    oldest_resuming_sync_replication_date = min(
                        self.parse_date(record.get(replication_key))
                        for record in resuming_sync_messages)
                    self.assertEqual(oldest_resuming_sync_replication_date,
                                     oldest_first_sync_replication_date)

                # verify that we get atleast 1 record in the resuming sync
                if stream not in ('collects'):
                    self.assertGreater(resuming_sync_count, 0, msg="Resuming sync yielded 0 recs")
                else:
                    self.assertEqual(resuming_sync_count, 0, msg="Resuming sync was not empty")
