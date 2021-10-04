import unittest
from unittest import mock
from singer.utils import strptime_to_utc
from tap_shopify.context import Context

INVENTORY_ITEM_OBJECT = Context.stream_objects['inventory_items']()

class Product():
    def __init__(self, id, variants):
        self.id = id
        self.variants = variants
        
class ProductVariant():
    def __init__(self, id, inventory_item_id):
        self.id = id
        self.inventory_item_id = inventory_item_id

class InventoryItems():
    def __init__(self, id, updated_at):
        self.id = id
        self.updated_at = updated_at
        
    def to_dict(self):
        return {"id": self.id, "updated_at": self.updated_at}

ITEM_1 = InventoryItems("i11", "2021-08-11T01:57:05-04:00")
ITEM_2 = InventoryItems("i12", "2021-08-12T01:57:05-04:00")
ITEM_3 = InventoryItems("i21", "2021-08-13T01:57:05-04:00")
ITEM_4 = InventoryItems("i22", "2021-08-14T01:57:05-04:00")
        
class TestInventoryItems(unittest.TestCase):

    @mock.patch("tap_shopify.streams.base.Stream.get_objects")
    @mock.patch("tap_shopify.streams.inventory_items.InventoryItems.get_inventory_items")
    def test_get_objects_with_product_variant(self, mock_get_inventory_items, mock_parent_object):

        expected_inventory_items =  [ITEM_1, ITEM_2, ITEM_3, ITEM_4]
        product1 = Product("p1", [ProductVariant("v11", "i11"), ProductVariant("v21", "i21")])
        product2 = Product("p2", [ProductVariant("v12", "i12"), ProductVariant("v22", "i22")])
        
        mock_get_inventory_items.side_effect = [[ITEM_1, ITEM_2], [ITEM_3, ITEM_4]]
        mock_parent_object.return_value = [product1, product2]

        actual_inventory_items = list(INVENTORY_ITEM_OBJECT.get_objects())
        
        #Verify that it returns inventory_item of all product variant
        self.assertEqual(actual_inventory_items, expected_inventory_items)
        
    
    @mock.patch("tap_shopify.streams.base.Stream.get_objects")
    @mock.patch("tap_shopify.streams.inventory_items.InventoryItems.get_inventory_items")
    def test_get_objects_with_product_but_no_variant(self, mock_get_inventory_items, mock_parent_object):
                
        expected_inventory_items =  [ITEM_3, ITEM_4]
        
        #Product1 contain no variant
        product1 = Product("p1", [])
        
        product2 = Product("p2", [ProductVariant("v12", "i12"), ProductVariant("v22", "i22")])
        mock_parent_object.return_value = [product1, product2]
        
        mock_get_inventory_items.side_effect = [[], [ITEM_3, ITEM_4]] 
        
        actual_inventory_items = list(INVENTORY_ITEM_OBJECT.get_objects())
        #Verify that it returns inventory_item of existing product variant
        self.assertEqual(actual_inventory_items, expected_inventory_items)
    
    
    @mock.patch("tap_shopify.streams.base.Stream.get_objects")
    @mock.patch("tap_shopify.streams.inventory_items.InventoryItems.get_inventory_items")
    def test_get_objects_with_no_product(self, mock_get_inventory_items, mock_parent_object):
        
        #No product exist
        mock_parent_object.return_value = []
        expected_inventory_items = []
        
        actual_inventory_items = list(INVENTORY_ITEM_OBJECT.get_objects())
        self.assertEqual(actual_inventory_items, expected_inventory_items)
        
    @mock.patch("tap_shopify.streams.base.Stream.get_bookmark")
    @mock.patch("tap_shopify.streams.inventory_items.InventoryItems.get_objects")
    def test_sync(self, mock_get_objects, mock_get_bookmark):
                
        expected_sync = [ITEM_3.to_dict(), ITEM_4.to_dict()]
        mock_get_objects.return_value = [ITEM_1, ITEM_2, ITEM_3, ITEM_4]
        
        mock_get_bookmark.return_value = strptime_to_utc("2021-08-13T01:05:05-04:00")
        
        actual_sync = list(INVENTORY_ITEM_OBJECT.sync())
        
        #Verify that only 2 record syncs
        self.assertEqual(actual_sync, expected_sync)