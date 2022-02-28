import re
import singer

# These are standard keys defined in the JSON Schema spec. We will not apply rules on these STANDARD_KEYS.
STANDARD_KEYS = [
    'selected',
    'inclusion',
    'description',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
    'maxLength',
    'minLength',
    'format',
    'type',
    'additionalProperties',
    'anyOf',
    'patternProperties',
]


LOGGER = singer.get_logger()

class RuleMap:
    GetStdFieldsFromApiFields = {}


    def fill_rule_map_object_by_catalog(self, stream_name, stream_metadata):
        """
        Read original-name of fields available in metadata of catalog and add it in `GetStdFieldsFromApiFields` dict object.
        param1:    stream_name: users
        param2:    stream_metadata
                    {
                        "breadcrumb": [
                            "properties",
                            "user_name"
                        ],
                        "metadata": {
                            "original-name": "UserName"
                        }
                    }

        After iterating all metadata,
        GetStdFieldsFromApiFields['users'] = {('properties', 'UserName'): 'user_name'}
        """
        self.GetStdFieldsFromApiFields[stream_name] = {}

        for key, value in stream_metadata.items():
            api_name = value.get('original-name')
            if api_name and key:
                self.GetStdFieldsFromApiFields[stream_name][key[:-1] + (api_name,)] = key[-1:][0]

    def apply_ruleset_on_schema(self, schema, stream_name, parent = ()):
        """
        Apply defined rule set on schema and return it.
        """
        temp_dict = {}
        roll_back_dict = {}

        if schema and isinstance(schema, dict) and schema.get('properties'):
            # Iterate through each item of schema.
            for key in schema['properties'].keys():
                breadcrumb = parent + ('properties', key)

                self.apply_ruleset_on_schema(schema['properties'][key], stream_name, breadcrumb)

                # Skip keys available in STANDARD_KEYS
                if key not in STANDARD_KEYS:

                    # apply rules on field
                    standard_key = self.apply_rules_to_original_field(key)

                    if key != standard_key: # Field name is changed after applying rules
                        if standard_key not in temp_dict: # Check if same standard name of field is already available or not at same level

                            # Add standard name of field in GetStdFieldsFromApiFields with key as tuple of breadcrumb keys
                            # Example: GetStdFieldsFromApiFields['users'][('properties', 'user_name')] = 'UserName'
                            self.GetStdFieldsFromApiFields[stream_name][parent + ('properties', standard_key)] = key

                            # Add key in temp_dict with value as standard_key to update in schema after iterating whole schema
                            # Because we can not update schema while iterating it.
                            temp_dict[key] = standard_key
                        else:
                            # Print Warning message for field name conflict found same level and add it's standard name to
                            # roll_back_dict because we need to roll back standard field name to original field name.
                            LOGGER.warning(f' Conflict found for field : {breadcrumb}')
                            roll_back_dict[standard_key] = True

        elif schema.get('anyOf'):
            # Iterate through each possible datatype of field
            # Example:
            # 'sources': {
            #       'anyOf': [
            #            {
            #                'type': ['null', 'array'],
            #                'items': {
            #                           'type': ['null', 'object'],
            #                           'properties': {}
            #               }
            #            },
            #            {
            #                'type': ['null', 'object'],
            #                'properties': {}
            #            }
            #       ]
            #
            # }
            for sc in schema.get('anyOf'):
                self.apply_ruleset_on_schema(sc, stream_name, parent)
        elif schema and isinstance(schema, dict) and schema.get('items'):
            breadcrumb = parent + ('items',)
            self.apply_ruleset_on_schema(schema['items'], stream_name, breadcrumb)

        for key, new_key in temp_dict.items():
            if roll_back_dict.get(new_key):
                breadcrumb = parent + ('properties', new_key)
                # Remove key with standard name from GetStdFieldsFromApiFields for which conflict was found.
                del self.GetStdFieldsFromApiFields[stream_name][breadcrumb]
                LOGGER.warning(f' Conflict found for field : {parent + ("properties", key)}')
            else:
                # Replace original name of field with standard name in schema
                schema['properties'][new_key] = schema['properties'].pop(key)

        return schema

    def apply_rule_set_on_stream_name(self, stream_name):
        """
        Apply defined rule set on stream name and return it.
        """
        standard_stream_name = self.apply_rules_to_original_field(stream_name)

        if stream_name != standard_stream_name:
            LOGGER.info(f'{stream_name}')
            self.GetStdFieldsFromApiFields[stream_name]['stream_name'] = stream_name
            return standard_stream_name

        # return original name of stream if it is not changed.
        return stream_name

    @classmethod
    def apply_rules_to_original_field(cls, key):
        """
        Apply defined rules on field.
        - Divide alphanumeric strings containing small letters followed by capital letters into multiple words and joined with underscores.
            - However, two or more adjacent capital letters are considered a part of one word.
            - Example:
                anotherName -> another_name
                ANOTHERName -> anothername
        - Divide alphanumeric strings containing letters and number into multiple words and joined with underscores.
            - Example:
                MyName123 -> my_name_123
        - Convert any character that is not a letter, digit, or underscore to underscore. A space is considered a character.
            - Example:
                A0a_*A -> a_0_a_a
        - Convert multiple underscores to a single underscore
            - Example:
                add____*LPlO -> add_lpl_o
        - Convert all upper-case letters to lower-case.
        """

        standard_key = re.findall('[A-Z]*[^A-Z]*', key)
        standard_key = '_'.join(standard_key)

        standard_key = re.findall(r'[A-Za-z]+|\d+', standard_key)
        standard_key = '_'.join(standard_key)

        return standard_key.lower()

    def apply_ruleset_on_api_response(self, response, stream_name, parent = ()):
        """
        Apply defined rule set on api response and return it.
        """
        temp_dict = {}
        if isinstance(response, dict):
            for key, value in response.items():
                if isinstance(response, list) and value:
                    if parent == ():
                        parent = parent  + ('properties', key)
                    breadcrumb = parent + ('items',)
                    # Iterate through each item of list
                    for vl in value:
                        self.apply_ruleset_on_api_response(vl, stream_name, breadcrumb)
                elif isinstance(response, dict):
                    breadcrumb = parent  + ('properties', key)
                    self.apply_ruleset_on_api_response(value, stream_name, breadcrumb)
                else:
                    breadcrumb = parent + ('properties', key)

                if breadcrumb in self.GetStdFieldsFromApiFields[stream_name]:
                    # Field found in the rule_map that need to be changed to standard name
                    temp_dict[key] = self.GetStdFieldsFromApiFields[stream_name][breadcrumb]

            # Updated key in record
            for key, new_key in temp_dict.items():
                # Replace original name of field with standard name in response
                response[new_key] = response.pop(key)

        return response