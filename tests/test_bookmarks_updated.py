"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
from datetime import datetime as dt

from dateutil.parser import parse

from tap_tester import menagerie, connections, runner
from base import BaseTapTest


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    @staticmethod
    def name():
        return "tap_tester_shopify_bookmark_update_test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2021-04-01T00:00:00Z'
    # We are currently working with 1 store as we do not have access to stitchdatawarehouse store
    # def test_run_store_1(self):
    #     with self.subTest(store="store_1"):
    #         conn_id = self.create_connection(original_credentials=True)
    #         self.bookmarks_test(conn_id, self.store_1_streams)

    # creating this global variable for store 2 which is required only for this test, all the other test are referencing from base
    global store_2_streams
    # removed 'abandoned_checkouts' from store 2 streams, as per the Doc:
    #   https://help.shopify.com/en/manual/orders/abandoned-checkouts?st_source=admin&st_campaign=abandoned_checkouts_footer&utm_source=admin&utm_campaign=abandoned_checkouts_footer#review-your-abandoned-checkouts
    # abandoned checkouts are saved in the Shopify admin for three months.
    # Every Monday, abandoned checkouts that are older than three months are removed from your admin.
    # Also no POST call is available for this endpoint: https://shopify.dev/api/admin-rest/2022-01/resources/abandoned-checkouts
    store_2_streams = {'metafields_products', 'transactions', 'order_refunds', 'products', 'locations', 'inventory_items', 'customers', 'collections', 'orders'}

    def test_run_store_2(self):
        with self.subTest(store="store_2"):
            conn_id = self.create_connection(original_properties=False, original_credentials=False)
            self.bookmarks_test(conn_id, store_2_streams)

    def bookmarks_test(self, conn_id, testable_streams):

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
        self.assertEqual(set(first_sync_record_count.keys()),
                         incremental_streams)

        first_sync_bookmark = menagerie.get_state(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        # BUG:TDL-17087 : State has additional values which are not streams
        # Need to remove additional values from bookmark value
        extra_stuff = {'transaction_orders', 'metafield_products', 'refund_orders', 'product_variants'}
        for keys in list(first_sync_bookmark['bookmarks'].keys()):
            if keys in extra_stuff:
                first_sync_bookmark['bookmarks'].pop(keys)

        #######################
        # Update State between Syncs
        #######################

        new_state = {'bookmarks': dict()}
       #simulated_states = self.calculated_states_by_stream(first_sync_bookmark)

        # We are hardcoding the updated state to ensure that we get atleast 1 record in second sync. These values have been provided after reviewing the max bookmark value for each of the streams
        simulated_states = {
            'products': {'updatedAt': '2025-01-23T14:08:21.000000Z'},
            'abandoned_checkouts': {'updatedAt': '2025-01-20T06:56:01.000000Z'},
            'inventory_levels': {'updatedAt': '2024-12-05T09:26:47.000000Z'},
            'locations': {'createdAt': '2021-07-29T08:38:44.000000Z'},
            'inventory_items': {'updatedAt': '2021-09-15T19:44:11.000000Z'},
            'transactions': {'createdAt': '2024-12-05T09:58:40.000000Z'},
            'metafields_customer': '2025-01-21T13:28:24.000000Z',
            'order_refunds': {'updatedAt': '2024-12-05T09:58:40.000000Z'},
            'customers': {'updatedAt': '2025-01-19T20:55:07.000000Z'},
            'orders': {'updatedAt': '2024-12-05T09:59:35.000000Z'},
            'collections': {'updatedAt': '2025-01-21T13:29:06.000000Z'},
            'metafields_products': {'updatedAt': '2025-01-21T03:11:54.000000Z'}
        }

        for stream, updated_state in simulated_states.items():
            new_state['bookmarks'][stream] = updated_state
        menagerie.set_state(conn_id, new_state)

        ###############################
        # Run Second Sync
        ###############################

        second_sync_record_count = self.run_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmark = menagerie.get_state(conn_id)

        for stream in testable_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = self.expected_replication_method()
                expected_replication_keys = self.expected_replication_keys()
                # information required for assertions from sync 1 and 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']

                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']

                first_bookmark_value = first_sync_bookmark.get('bookmarks', {stream: None}).get(stream)
                first_bookmark_value = list(first_bookmark_value.values())[0]
                second_bookmark_value = second_sync_bookmark.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = list(second_bookmark_value.values())[0]

                replication_key = next(iter(expected_replication_keys[stream]))
                first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                simulated_bookmark = new_state['bookmarks'][stream]
                simulated_bookmark_value = list(simulated_bookmark.values())[0]

                # verify the syncs sets a bookmark of the expected form
                self.assertIsNotNone(first_bookmark_value)
                self.assertTrue(self.is_expected_date_format(first_bookmark_value))
                self.assertIsNotNone(second_bookmark_value)
                self.assertTrue(self.is_expected_date_format(second_bookmark_value))

                # verify the 2nd bookmark is equal to 1st sync bookmark
                #NOT A BUG (IS the expected behaviour for shopify as they are using date windowing : TDL-17096 : 2nd bookmark value is getting assigned from the execution time rather than the actual bookmark time. This is an invalid assertion for shopify
                #self.assertEqual(first_bookmark_value, second_bookmark_value)

                for record in first_sync_messages:
                    replication_key_value = record.get(replication_key)
                    # verify 1st sync bookmark value is the max replication key value for a given stream
                    self.assertLessEqual(replication_key_value, first_bookmark_value_utc, msg="First sync bookmark was set incorrectly, a record with a greater replication key value was synced")

                for record in second_sync_messages:
                    replication_key_value = record.get(replication_key)
                    # verify the 2nd sync replication key value is greater or equal to the 1st sync bookmarks
                    self.assertGreaterEqual(self.convert_state_to_utc(replication_key_value), simulated_bookmark_value, msg="Second sync records do not respect the previous                                                  bookmark")
                    # verify the 2nd sync bookmark value is the max replication key value for a given stream
                    self.assertLessEqual(replication_key_value, second_bookmark_value_utc, msg="Second sync bookmark was set incorrectly, a record with a greater replication key value was synced")

                # verify that we get less data in the 2nd sync
                self.assertLess(second_sync_count, first_sync_count,
                                msg="Second sync does not have less records, bookmark usage not verified")

                # verify that we get atleast 1 record in the second sync
                self.assertGreater(second_sync_count, 0, msg="Second sync did not yield any records")
