import unittest
from unittest import mock
from singer.utils import strptime_to_utc
from tap_shopify.context import Context

INVENTORY_LEVEL_OBJECT = Context.stream_objects['inventory_levels']()

class Location():
    def __init__(self, id):
        self.id = id

class InventoryLevels():
    def __init__(self, id, updated_at):
        self.id = id
        self.updated_at = updated_at

    def to_dict(self):
        return {"id": self.id, "updated_at": self.updated_at}

LEVEL_1 = InventoryLevels("inv_level1", "2021-08-11T01:57:05-04:00")
LEVEL_2 = InventoryLevels("inv_level2", "2021-08-12T01:57:05-04:00")
LEVEL_3 = InventoryLevels("inv_level3", "2021-08-13T01:57:05-04:00")
LEVEL_4 = InventoryLevels("inv_level4", "2021-08-14T01:57:05-04:00")

@mock.patch("tap_shopify.streams.base.Stream.get_bookmark")
class TestInventoryItems(unittest.TestCase):

    @mock.patch("tap_shopify.streams.locations.Locations.get_locations_data")
    @mock.patch("tap_shopify.streams.inventory_levels.InventoryLevels.get_inventory_levels")
    def test_get_objects_with_locations(self, mock_get_inventory_levels, mock_parent_object, mock_get_bookmark):
        '''
            Verify that expected data should be emitted for inventory_levels if locations found.
        '''
        expected_inventory_levels =  [LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4]
        location1 = Location("location1")
        location2 = Location("location2")

        mock_get_inventory_levels.side_effect = [[LEVEL_1, LEVEL_2], [LEVEL_3, LEVEL_4]]
        mock_parent_object.return_value = [location1, location2]

        actual_inventory_levels = list(INVENTORY_LEVEL_OBJECT.get_objects())

        #Verify that it returns inventory_levels for all locations
        self.assertEqual(actual_inventory_levels, expected_inventory_levels)

    @mock.patch("tap_shopify.streams.locations.Locations.get_locations_data")
    @mock.patch("tap_shopify.streams.inventory_levels.InventoryLevels.get_inventory_levels")
    def test_get_objects_with_no_locations(self, mock_get_inventory_levels, mock_parent_object, mock_get_bookmark):
        '''
            Verify that no data should be emitted for inventory_levels if no locations found.
        '''
        # No data for parent stream location
        mock_parent_object.return_value = []
        expected_inventory_levels = []

        actual_inventory_levels = list(INVENTORY_LEVEL_OBJECT.get_objects())

        # No get_inventory_levels should be called and no data should be returned 
        self.assertEqual(actual_inventory_levels, expected_inventory_levels)
        self.assertEqual(mock_get_inventory_levels.call_count, 0)

    @mock.patch("tap_shopify.streams.inventory_levels.InventoryLevels.get_objects")
    def test_sync(self, mock_get_objects, mock_get_bookmark):
        '''
            Verify that only data updated after specific bookmark are yielded from sync.
        '''

        expected_sync = [LEVEL_3.to_dict(), LEVEL_4.to_dict()]
        mock_get_objects.return_value = [LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4]

        mock_get_bookmark.return_value = strptime_to_utc("2021-08-13T01:05:05-04:00")

        actual_sync = list(INVENTORY_LEVEL_OBJECT.sync())

        #Verify that only 2 record syncs
        self.assertEqual(actual_sync, expected_sync)
