import os

from tap_tester import runner, menagerie
from base import BaseTapTest

class AllFieldsTest(BaseTapTest):

    @staticmethod
    def name():
        return "tap_tester_shopify_all_fields_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2021-04-01T00:00:00Z',
            'shop': 'talenddatawearhouse',
            'date_window_size': 30
        }

        return return_value

    @staticmethod
    def get_credentials(original_credentials: bool = True):
        """Authentication information for the test account"""

        return {
            'api_key': os.getenv('TAP_SHOPIFY_API_KEY_TALENDDATAWEARHOUSE')
        }

    def test_run(self):
        """
        Ensure running the tap with all streams and fields selected results in the
        replication of all fields.
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """

        expected_streams = self.expected_streams()

        # instantiate connection
        conn_id = self.create_connection()

        # run check mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('stream_name') in expected_streams]
        self.select_all_streams_and_fields(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys().get(stream, set()) | self.expected_replication_keys().get(stream, set())
                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                if stream == 'abandoned_checkouts':
                    expected_all_keys.remove('billing_address')
                elif stream == 'orders':
                    # No field named 'order_adjustments' present in the 'order' object
                    #   Documentation: https://shopify.dev/api/admin-rest/2021-10/resources/order#resource_object
                    expected_all_keys.remove('order_adjustments')

                self.assertSetEqual(expected_all_keys, actual_all_keys)
