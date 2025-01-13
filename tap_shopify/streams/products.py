from tap_shopify.streams.graphql_base import GraphQLStream
from tap_shopify.context import Context

class Products(GraphQLStream):
    name = 'products'
    
    # Field mapping from catalog to GraphQL fields
    FIELD_MAPPINGS = {
        'available_publications_count': {
            'field': 'availablePublicationsCount',
            'subfields': ['count', 'precision']
        },
        'created_at': 'createdAt',
        'default_cursor': 'defaultCursor',
        'description_html': 'descriptionHtml',
        'description': 'description',
        'gift_card_template_suffix': 'giftCardTemplateSuffix',
        'handle': 'handle',
        'has_only_default_variant': 'hasOnlyDefaultVariant',
        'has_out_of_stock_variants': 'hasOutOfStockVariants',
        'has_variants_that_requires_components': 'hasVariantsThatRequiresComponents',
        'id': 'id',
        'is_gift_card': 'isGiftCard',
        'legacy_resource_id': 'legacyResourceId',
        'media': {
            'field': 'media',
            'arguments': '(first: 2)',
            'subfields': {
                'id': 'id',
                'alt': 'alt',
                'media_content_type': 'mediaContentType',
                'status': 'status',
                'video': {
                    'fragment': 'Video',
                    'fields': [
                        'createdAt',
                        'duration',
                        'filename',
                        'fileStatus',
                        'updatedAt'
                    ]
                },
                'image': {
                    'fragment': 'MediaImage',
                    'fields': [
                        'createdAt',
                        'fileStatus',
                        'mimeType',
                        'updatedAt'
                    ]
                }
            }
        },
        'title': 'title',
        'media_count': {
            'field': 'mediaCount',
            'subfields': ['count', 'precision']
        },
        'online_store_preview_url': 'onlineStorePreviewUrl',
        'online_store_url': 'onlineStoreUrl',
        'product_type': 'productType',
        'published_at': 'publishedAt',
        'published_on_current_publication': 'publishedOnCurrentPublication',
        'requires_selling_plan': 'requiresSellingPlan',
        'resource_publication_on_current_publication': {
            'field': 'resourcePublicationOnCurrentPublication',
            'subfields': ['isPublished', 'publishDate']
        },
        'status': 'status',
        'tags': 'tags',
        'template_suffix': 'templateSuffix',
        'total_inventory': 'totalInventory',
        'tracks_inventory': 'tracksInventory',
        'updated_at': 'updatedAt',
        'vendor': 'vendor',
        'variants': {
            'field': 'variants',
            'arguments': '(first: 150)',
            'subfields': [
                'availableForSale',
                'barcode',
                'compareAtPrice',
                'createdAt',
                'defaultCursor',
                'displayName',
                'id',
                'inventoryPolicy',
                'inventoryQuantity',
                'legacyResourceId',
                'position',
                'price',
                'requiresComponents',
                'sellableOnlineQuantity',
                'sku',
                'taxCode',
                'taxable',
                'title',
                'updatedAt'
            ]
        },
        'resource_publications_count': {
            'field': 'resourcePublicationsCount',
            'subfields': ['count', 'precision']
        }
    }

    def build_field_selection(self, field_mapping, selected_fields=None):
        """Recursively build GraphQL field selection"""
        if isinstance(field_mapping, str):
            return field_mapping

        if isinstance(field_mapping, dict):
            field = field_mapping['field']
            parts = [field]
            
            # Add arguments if present
            if 'arguments' in field_mapping:
                parts.append(field_mapping['arguments'])

            # Handle subfields
            if 'subfields' in field_mapping:
                subfields = field_mapping['subfields']
                
                if isinstance(subfields, list):
                    subfield_str = ' '.join(subfields)
                elif isinstance(subfields, dict):
                    sub_parts = []
                    for key, value in subfields.items():
                        if isinstance(value, dict) and 'fragment' in value:
                            # Handle fragments (like Video and MediaImage)
                            fragment_fields = ' '.join(value['fields'])
                            sub_parts.append(f'... on {value["fragment"]} {{ {fragment_fields} }}')
                        else:
                            sub_parts.append(self.build_field_selection(value))
                    subfield_str = ' '.join(sub_parts)
                else:
                    subfield_str = ' '.join(subfields)

                parts.append(f'{{ {subfield_str} }}')
                
            return ' '.join(parts)

        return ''

    def get_selected_fields(self):
        """Get list of selected fields from catalog"""
        mdata = Context.metadata.get(('properties', self.name), {})
        selected_fields = []
        
        for field, mapping in self.FIELD_MAPPINGS.items():
            if mdata.get(('properties', field, 'selected'), True):  # Default to True if not specified
                selected_fields.append(field)
                
        return selected_fields

    def build_query(self, selected_fields):
        """Build the complete GraphQL query"""
        field_selections = []
        
        for field in selected_fields:
            mapping = self.FIELD_MAPPINGS.get(field)
            if mapping:
                field_selection = self.build_field_selection(mapping)
                if field_selection:
                    field_selections.append(field_selection)

        # Always include pagination info and necessary fields
        required_fields = """
            pageInfo {
                hasNextPage
                endCursor
            }
        """
        
        fields_str = '\n'.join(field_selections)
        
        return f"""
        query($first: Int!, $after: String) {{
            products(first: $first, after: $after) {{
                {required_fields}
                edges {{
                    node {{
                        {fields_str}
                    }}
                }}
            }}
        }}
        """

    def get_graphql_query(self):
        """Generate the GraphQL query based on selected fields"""
        selected_fields = self.get_selected_fields()
        return self.build_query(selected_fields)

    def get_connection_from_data(self, data):
        return data['products']

    def process_node(self, node):
        """Process the node based on selected fields"""
        selected_fields = self.get_selected_fields()
        processed = {}
        
        for field in selected_fields:
            if field in node:
                processed[field] = node[field]
                
        return processed

Context.stream_objects['products'] = Products
