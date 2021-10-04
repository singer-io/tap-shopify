import tap_shopify
import unittest

class TestShopInfo(unittest.TestCase):

    def test_shop_info_schema(self):
        schema = {
            'properties': {
                'field_1': {
                    'type': ['null', 'string']
                },
                'field_2': {
                    'type': ['null', 'integer']
                },
                'field_3': {
                    'type': ['null', 'string']
                }
            },
            'type': 'object'
        }
        schema_with_shop_info = tap_shopify.add_synthetic_key_to_schema(schema)
        actual_keys = schema_with_shop_info.get('properties').keys()
        expected_keys = ['_sdc_shop_id', '_sdc_shop_name', '_sdc_shop_myshopify_domain']
        for key in expected_keys:
            self.assertTrue(key in actual_keys)
