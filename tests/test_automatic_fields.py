"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie

from base import BaseTapTest


class MinimumSelectionTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_shopify_no_fields_test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2021-04-01T00:00:00Z'

    def test_run(self):
        with self.subTest(store="store_1"):
            conn_id = self.create_connection(original_credentials=True)
            self.automatic_test(conn_id, self.store_1_streams)

        with self.subTest(store="store_2"):
            conn_id = self.create_connection(original_properties=False, original_credentials=False)
            self.automatic_test(conn_id, self.store_2_streams)

    def automatic_test(self, conn_id, testable_streams):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.
        Verify that all replicated records have unique primary key values.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL and key in testable_streams}

        # Select all streams and no fields within streams
        # IF THERE ARE NO AUTOMATIC FIELDS FOR A STREAM
        # WE WILL NEED TO UPDATE THE BELOW TO SELECT ONE
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()
        synced_records = runner.get_records_from_target_output()

        for stream in incremental_streams:
            with self.subTest(stream=stream):

                # verify that you get more than a page of data
                # SKIP THIS ASSERTION FOR STREAMS WHERE YOU CANNOT GET
                # MORE THAN 1 PAGE OF DATA IN THE TEST ACCOUNT
                stream_metadata = self.expected_metadata().get(stream, {})
                expected_primary_keys = self.expected_primary_keys().get(stream, set())

                # collect records
                messages = synced_records.get(stream)

                minimum_record_count = stream_metadata.get(
                    self.API_LIMIT,
                    self.get_properties().get('result_per_page', self.DEFAULT_RESULTS_PER_PAGE)
                )
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    minimum_record_count,
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target
                self.assertEqual(
                    actual_fields_by_stream.get(stream, set()),
                    expected_primary_keys |
                    self.expected_replication_keys().get(stream, set()),
                    msg="The fields sent to the target are not the automatic fields"
                )

                # Verify that all replicated records have unique primary key values.
                records_pks_set = {tuple([message.get('data').get(primary_key) for primary_key in expected_primary_keys])
                                          for message in messages.get('messages')}
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_primary_keys])
                                           for message in messages.get('messages')]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
