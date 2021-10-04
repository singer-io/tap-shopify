import unittest
from unittest import mock
from singer import utils
from singer.utils import strptime_to_utc, strftime
from tap_shopify.context import Context

LOCATIONS_OBJECT = Context.stream_objects['locations']()


class Locations():
    def __init__(self, id, updated_at):
        self.id = id
        self.updated_at = updated_at

    def to_dict(self):
        return {"id": self.id, "updated_at": self.updated_at}


LOCATION_1 = Locations("i11", "2021-08-11T01:57:05-04:00")
LOCATION_2 = Locations("i12", "2021-08-12T01:57:05-04:00")
LOCATION_3 = Locations("i21", "2021-08-13T01:57:05-04:00")
LOCATION_4 = Locations("i22", "2021-08-14T01:57:05-04:00")


class TestLocations(unittest.TestCase):
    @mock.patch("tap_shopify.streams.base.Stream.update_bookmark")
    @mock.patch("tap_shopify.streams.base.Stream.get_bookmark")
    @mock.patch("tap_shopify.streams.locations.Locations.get_locations_data")
    def test_sync(self, mock_get_locations_data, mock_get_bookmark, mock_update_bookmark):

        expected_sync = [LOCATION_3.to_dict(), LOCATION_4.to_dict()]
        mock_get_locations_data.return_value = [LOCATION_1, LOCATION_2, LOCATION_3, LOCATION_4]

        mock_get_bookmark.return_value = strptime_to_utc("2021-08-13T01:05:05-04:00")

        actual_sync = list(LOCATIONS_OBJECT.sync())

        # Verify that only 2 record syncs
        self.assertEqual(actual_sync, expected_sync)
        max_bookmark = strptime_to_utc("2021-08-14T01:57:05-04:00")

        # Verify that maximum replication key of all keys is updated as bookmark
        mock_update_bookmark.assert_called_with(utils.strftime(max_bookmark))
