from singer import metadata
from tap_shopify.streams.graphql_base import GraphQLStream
from tap_shopify.context import Context

class Products(GraphQLStream):
    name = 'products'

    def get_selected_fields(self):
        """Get list of selected fields from catalog."""
        mdata = None
        schema = {}
        for stream in Context.catalog['streams']:
            if stream['stream'] == self.name:
                mdata = metadata.to_map(stream['metadata'])
                schema = stream['schema']
        selected_fields = []

        if 'properties' in schema.keys():
            for field in schema['properties'].keys():
                if mdata.get(('properties', field, 'selected'), True):  # Default to True if not specified
                    selected_fields.append(field)

        return selected_fields, schema

    # def build_field_string(self, schema, selected_fields):
    #     """Recursively build the field string for GraphQL."""
    #     field_strings = []

    #     for field in selected_fields:
    #         field_schema = schema['properties'].get(field)
    #         if not field_schema:
    #             continue

    #         # Check if the field is an object with nested properties
    #         if field_schema.get("type") == "object" and "properties" in field_schema:
    #             nested_fields = list(field_schema["properties"].keys())
    #             nested_field_string = self.build_field_string(field_schema, nested_fields)
    #             if nested_field_string:
    #                 field_strings.append(f"{field} {{\n{nested_field_string}\n}}")
    #         elif field_schema.get("type") == "array" and "items" in field_schema:
    #             # Handle array fields with nested objects
    #             if field_schema["items"].get("type") == "object" and "properties" in field_schema["items"]:
    #                 nested_fields = list(field_schema["items"]["properties"].keys())
    #                 nested_field_string = self.build_field_string(field_schema["items"], nested_fields)
    #                 if nested_field_string:
    #                     field_strings.append(f"{field}(first: 2) {{\n  edges {{\n    node {{\n{nested_field_string}\n    }}\n  }}\n  pageInfo {{\n    endCursor\n    hasNextPage\n  }}\n}}")
    #         else:
    #             # Base field
    #             field_strings.append(field)

    #     return "\n".join(field_strings)

    # def build_query(self):
    #     """Build the complete GraphQL query."""
    #     selected_fields, schema = self.get_selected_fields()

    #     # Always include pagination info
    #     pagination_info = """
    #         pageInfo {
    #             hasNextPage
    #             endCursor
    #         }
    #     """

    #     # Build field string dynamically
    #     field_string = self.build_field_string(schema, selected_fields)

    #     return f"""
    #     query($first: Int!, $after: String) {{
    #         products(first: $first, after: $after) {{
    #             {pagination_info}
    #             edges {{
    #                 node {{
    #                     {field_string}
    #                 }}
    #             }}
    #         }}
    #     }}
    #     """

    def get_graphql_query(self):
        return """
        query GetProducts($first: Int!, $after: String, $query: String) {
            products(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        availablePublicationsCount {
                            count
                            precision
                        }
                        createdAt
                        descriptionHtml
                        description
                        giftCardTemplateSuffix
                        handle
                        hasOnlyDefaultVariant
                        hasOutOfStockVariants
                        hasVariantsThatRequiresComponents
                        id
                        isGiftCard
                        legacyResourceId
                        media(first: 2) {
                            edges {
                                node {
                                    id
                                    alt
                                    mediaContentType
                                    status
                                    ... on Video {
                                        id
                                        alt
                                        createdAt
                                        duration
                                        filename
                                        fileStatus
                                        mediaContentType
                                        status
                                        updatedAt
                                    }
                                    ... on MediaImage {
                                        id
                                        alt
                                        createdAt
                                        fileStatus
                                        mediaContentType
                                        mimeType
                                        status
                                        updatedAt
                                    }
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                        title
                        mediaCount {
                            count
                            precision
                        }
                        onlineStorePreviewUrl
                        onlineStoreUrl
                        productType
                        publishedAt
                        requiresSellingPlan
                        resourcePublicationOnCurrentPublication {
                            isPublished
                            publishDate
                        }
                        status
                        tags
                        templateSuffix
                        totalInventory
                        tracksInventory
                        updatedAt
                        vendor
                        variants(first: 250) {
                            edges {
                                node {
                                    availableForSale
                                    barcode
                                    compareAtPrice
                                    createdAt
                                    defaultCursor
                                    displayName
                                    id
                                    inventoryPolicy
                                    inventoryQuantity
                                    legacyResourceId
                                    position
                                    price
                                    requiresComponents
                                    sellableOnlineQuantity
                                    sku
                                    taxCode
                                    taxable
                                    title
                                    updatedAt
                                }
                            }
                        }
                        resourcePublicationsCount {
                            count
                            precision
                        }
                        options(first: 200) {
                            id
                            name
                            position
                            values
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

    def get_connection_from_data(self, data):
        return data['products']

    def transform_media(self, node):
        """
        Transform media structure from edges/node format to a flat array.
    
        Args:
            node (dict): The input JSON data
            
        Returns:
            dict: Transformed data with flattened media array
        """
        if "media" in node and "edges" in node["media"]:
            # Flatten the edges into a list of nodes
            node["media"] = [edge["node"] for edge in node["media"]["edges"]]

    def transform_variants(self, node):
        """
        Transform variant structure from edges/node format to a flat array.
    
        Args:
            node (dict): The input JSON data
            
        Returns:
            dict: Transformed data with flattened variants array
        """
        if "variants" in node and "edges" in node["variants"]:
            # Flatten the edges into a list of nodes
            node["variants"] = [edge["node"] for edge in node["variants"]["edges"]]

    def process_node(self, node):
        """Process the node based on selected fields."""
        selected_fields, _ = self.get_selected_fields()
        if "media" in selected_fields:
            self.transform_media(node)
        if "variants" in selected_fields:
            self.transform_variants(node)
        
        return node


Context.stream_objects['products'] = Products
