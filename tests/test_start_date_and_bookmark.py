import os
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import BaseTapTest

class TestStartDateAndBookmark(BaseTapTest):
    """Test bookmark is used if start date and bookmark both provided in sync"""

    def name(self):
        return "tap_tester_shopify_startdate_bookmark_test"


    def get_properties(self, original: bool = True):
        return {
            'start_date': '2017-07-01T00:00:00Z',
            'shop': 'talenddatawearhouse',
            'date_window_size': 30,
            'results_per_page': '50'
        }
    
    @staticmethod
    def get_credentials(original_credentials: bool = True):
        return {
            'api_key': os.getenv('TAP_SHOPIFY_API_KEY_TALENDDATAWEARHOUSE')
        }

    def test_run(self):
        conn_id = self.create_connection()

        # Select all incremental streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Set state with bookmark greater than start date
        state = dict()
        original_bookmark_value = '2021-04-01T00:00:00Z'
        state['bookmarks'] = {stream: {next(iter(self.expected_replication_keys()[stream])): original_bookmark_value}
                                  for stream in incremental_streams}

        menagerie.set_state(conn_id, state)
        
        # Run a sync job using orchestrator
        sync_record_count = self.run_sync(conn_id)

        # Synced records
        sync_records = runner.get_records_from_target_output()

        sync_bookmarks = menagerie.get_state(conn_id)

        for stream in incremental_streams:
            with self.subTest(stream=stream):
                # record messages
                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                replication_key = next(iter(self.expected_replication_keys()[stream]))

                # Verify that replication key value for all the data are greater than bookmark key 
                # so it verify that start date is not considered.
                for message in sync_messages:
                    replication_key_value = message.get('data').get(replication_key)
                    self.assertLess(original_bookmark_value, replication_key_value,
                                    msg="Record with lesser replication key value than bookmark was found.")
