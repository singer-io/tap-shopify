import os
import uuid
import singer
import unittest

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from base import ShopifyTest

from functools import reduce
from singer import metadata

LOGGER = singer.get_logger()

# The token used to authenticate our requests was generated on
# [2018-09-18](https://github.com/stitchdata/environments/commit/82609cef972fd631c628b8eb733f37eea8f5d4f4).
# If it ever expires, you'll need to login to Shopify via the 1Password
# login creds, setup a Shopify integration on your VM, and copy the new
# token out of the connections credentials into the environments repo.
class ShopifySyncRows(ShopifyTest):

    @staticmethod
    def name():
        return "tap_tester_shopify_sync_rows"

    def expected_sync_streams(self):
        return {
            'orders',
        }

    def expected_pks(self):
        return {
            'orders': {'id'},
        }

    def test_run(self):
        conn_id = self.create_connection()

        # Select our catalogs
        our_catalogs = [c for c in self.found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], [])

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Ensure all records have a value for PK(s)
        records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            messages = records.get(stream).get('messages')
            for m in messages:
                pk_set = self.expected_pks()[stream]
                for pk in pk_set:
                    self.assertIsNotNone(m.get('data', {}).get(pk), msg="oh no! {}".format(m))

        bookmarks = menagerie.get_state(conn_id)['bookmarks']

        self.assertTrue('orders' in bookmarks)
