import unittest
from tap_shopify.rule_map import RuleMap

FIELDS_SET = {
    '%MyName123': '_my_name_123',
    'ANOTHERName': 'anothername',
    'anotherName': 'another_name',
    'add____*LPlO': 'add_lpl_o',
    '123Abc%%_opR': '_123_abc_op_r',
    'UserName': 'user_name',
    'A0a_A': 'a_0_a_a',
    'aE0': 'a_e_0',
    'a.a b': 'a_a_b',
    '1MyName': '_1_my_name',
    '!MyName': '_my_name',
    'My_Name_': 'my_name_',
    '_My_Name': '_my_name',
    '___999Myy': '_999_myy',
    'My Name': 'my_name',
    '["_"]': '_',
    'test-abc': 'test_abc',
    '53234': '_53234',
    'My_Name!': 'my_name_',
    'My_Name____c': 'my_name_c',
    '1MyName': '_1_my_name',
    'blurry-green-dodo-important': 'blurry_green_dodo_important',
    '__new__--test__': '_new_test_',
    '-5.490030': '_5_490030',
    '\'aa\'': '_aa_',
    "Audience Report": 'audience_report',
    '******': '_',
    '``~Qo': '_qo',
    '99j_J': '_99_j_j'


}

class TestRuleMap(unittest.TestCase):

    def test_apply_rules_to_original_field(self):
        rule_map = RuleMap()

        for field, value in FIELDS_SET.items():
            standard_field = rule_map.apply_rules_to_original_field(field)
            self.assertEquals(standard_field, value)
