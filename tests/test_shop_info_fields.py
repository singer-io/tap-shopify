"""
Test tap discovery
"""
import re

from tap_tester import menagerie, runner

from base import BaseTapTest


class ShopInfoFieldsTest(BaseTapTest):
    """ Test the Shop Information Fields """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = '2021-04-01T00:00:00Z'

    @staticmethod
    def name():
        return "tap_tester_shopify_shop_info_fields_test"

    def test_run(self):
        """
            Verify shop information fields are present in catalog for every streams.
            Verify shop information fields are present in every records of all streams.
        """
        conn_id = self.create_connection(original_properties=False, original_credentials=False)
        # Select all streams and all fields within streams and run both mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in self.expected_streams()]

        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)
        sync_records_count = self.run_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        expected_shop_info_fields = {'_sdc_shop_id', '_sdc_shop_name', '_sdc_shop_myshopify_domain'}

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                
                # Verify that every stream schema contains shop info fields
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                actual_stream_fields = {item.get("breadcrumb", ["properties", None])[1]
                                        for item in metadata
                                        if item.get("breadcrumb", []) != []}

                self.assertTrue(expected_shop_info_fields.issubset(actual_stream_fields))

                # Verify that every records of stream contains shop info fields
                stream_records = sync_records.get(stream, {})
                upsert_messages = [m for m in stream_records.get('messages') if m['action'] == 'upsert']

                for message in upsert_messages:
                    actual_record_fields = set(message['data'].keys())
                    self.assertTrue(expected_shop_info_fields.issubset(actual_record_fields))
