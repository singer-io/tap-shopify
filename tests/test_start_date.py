"""
Test that the start_date configuration is respected
"""

from functools import reduce

import os

from dateutil.parser import parse

from tap_tester import menagerie, runner, LOGGER

from base import BaseTapTest


class StartDateTest(BaseTapTest):
    """
    Test that the start_date configuration is respected

    • verify that a sync with a later start date has at least one record synced
      and less records than the 1st sync with a previous start date
    • verify that each stream has less records than the earlier start date sync
    • verify all data from later start data has bookmark values >= start_date
    • verify that the minimum bookmark sent to the target for the later start_date sync
      is greater than or equal to the start date
    """

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date': '2021-04-01T00:00:00Z',
            'shop': 'talenddatawearhouse',
            'date_window_size': 30,
            # BUG: https://jira.talendforge.org/browse/TDL-13180
            'results_per_page': '50'
        }

        if original:
            return return_value

        return_value["start_date"] = '2021-04-21T00:00:00Z'
        return return_value

    @staticmethod
    def get_credentials(original_credentials: bool = True):
        return {
            'api_key': os.getenv('TAP_SHOPIFY_API_KEY_TALENDDATAWEARHOUSE')
        }

    @staticmethod
    def name():
        return "tap_tester_shopify_start_date_test"

    def test_run(self):
        """Test we get a lot of data back based on the start date configured in base"""
        conn_id = self.create_connection()

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        # removed 'abandoned_checkouts', as per the Doc:
        #   https://help.shopify.com/en/manual/orders/abandoned-checkouts?st_source=admin&st_campaign=abandoned_checkouts_footer&utm_source=admin&utm_campaign=abandoned_checkouts_footer#review-your-abandoned-checkouts
        # abandoned checkouts are saved in the Shopify admin for three months.
        # Every Monday, abandoned checkouts that are older than three months are removed from your admin.
        # Also no POST call is available for this endpoint: https://shopify.dev/api/admin-rest/2022-01/resources/abandoned-checkouts
        expected_replication_method = self.expected_replication_method()
        expected_replication_method.pop("abandoned_checkouts")
        incremental_streams = {key for key, value in expected_replication_method.items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)
        first_total_records = reduce(lambda a, b: a + b, first_sync_record_count.values())

        # Count actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # set the start date for a new connection based off bookmarks largest value
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        bookmark_list = [next(iter(book.values())) for stream, book in first_max_bookmarks.items()]
        bookmark_dates = []
        for bookmark in bookmark_list:
            try:
                bookmark_dates.append(parse(bookmark))
            except (ValueError, OverflowError, TypeError):
                pass

        if not bookmark_dates:
            # THERE WERE NO BOOKMARKS THAT ARE DATES.
            # REMOVE CODE TO FIND A START DATE AND ENTER ONE MANUALLY
            raise ValueError

        self.start_date = self.get_properties(original=False)["start_date"]

        # create a new connection with the new start_date
        conn_id = self.create_connection(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)
        second_total_records = reduce(lambda a, b: a + b, second_sync_record_count.values(), 0)
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # verify that at least one record synced and less records synced than the 1st connection
        self.assertGreater(second_total_records, 0)
        self.assertLess(second_total_records, first_total_records)

        for stream in incremental_streams:
            with self.subTest(stream=stream):

                # get primary key values for both sync records
                expected_primary_keys = self.expected_primary_keys()[stream]
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in first_sync_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in second_sync_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # verify that each stream has less records than the first connection sync
                self.assertGreaterEqual(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second had more records, start_date usage not verified")

                # Verify by primary key values, that all records of the 2nd sync are included in the 1st sync since 2nd sync has a later start date.
                self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                # verify all data from both syncs >= start_date
                first_sync_target_mark = first_min_bookmarks.get(stream, {"mark": None})
                second_sync_target_mark = second_min_bookmarks.get(stream, {"mark": None})

                # get start dates for both syncs
                first_sync_start_date = self.get_properties()["start_date"]
                second_sync_start_date = self.start_date

                for start_date, target_mark in zip((first_sync_start_date, second_sync_start_date), (first_sync_target_mark, second_sync_target_mark)):
                    target_value = next(iter(target_mark.values()))  # there should be only one

                    if target_value:

                        # it's okay if there isn't target data for a stream
                        try:
                            target_value = self.local_to_utc(parse(target_value))

                            # verify that the minimum bookmark sent to the target for the second sync
                            # is greater than or equal to the start date
                            self.assertGreaterEqual(target_value,
                                                    self.local_to_utc(parse(start_date)))

                        except (OverflowError, ValueError, TypeError):
                            LOGGER.warn(
                                "bookmarks cannot be converted to dates, can't test start_date for %s", stream
                            )
