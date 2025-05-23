import unittest
from unittest.mock import patch
import json
from graphql import parse, FieldNode, SelectionSetNode
from pathlib import Path
from tap_shopify.streams.orders import Orders
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
from textwrap import dedent

SCHEMA_PATH = Path("tap_shopify/schemas/orders.json")

def load_schema_fields(schema, prefix=""):
    """Recursively extract schema field paths."""
    fields = set()
    for key, value in schema.get("properties", {}).items():
        full_key = f"{prefix}.{key}" if prefix else key
        fields.add(full_key)
    return fields


def extract_top_level_fields_from_node(selection_set: SelectionSetNode, path=["orders", "edges", "node"]):
    """Extract top-level field names from a selection set under the given field path."""

    def find_selection(selections, name):
        for sel in selections:
            if isinstance(sel, FieldNode) and sel.name.value == name:
                return sel
        return None

    current_set = selection_set
    for key in path:
        node = find_selection(current_set.selections, key)
        if node is None or node.selection_set is None:
            return set()
        current_set = node.selection_set

    return {
        field.name.value
        for field in current_set.selections
        if isinstance(field, FieldNode)
    }

class TestGraphQLSchemaMatch(unittest.TestCase):
    def test_schema_matches_graphql_query(self):
        # Load and parse schema
        schema = json.loads(SCHEMA_PATH.read_text())
        schema_fields = load_schema_fields(schema)

        # Load and parse GraphQL query
        query_str = Orders().get_query()
        ast = parse(query_str)
        query_fields = set()

        for definition in ast.definitions:
            if hasattr(definition, "selection_set") and isinstance(definition.selection_set, SelectionSetNode):
                query_fields.update(extract_top_level_fields_from_node(definition.selection_set))

        # Compare fields
        missing_in_query = schema_fields - query_fields
        missing_in_schema = query_fields - schema_fields

        self.assertFalse(missing_in_query, f"Schema fields missing in query: {missing_in_query}")
        self.assertFalse(missing_in_schema, f"Query fields missing in schema: {missing_in_schema}")

    @patch('singer.metadata.to_map')
    @patch.object(Context, 'get_catalog_entry')
    def test_get_unselected_fields(self, mock_get_catalog_entry, mock_to_map):
        # Mock schema
        mock_schema = {
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string"},
                "created_at": {"type": "string"},
            }
        }

        # Mock metadata (breadcrumb: metadata dict)
        mock_metadata_map = {
            (): {"inclusion": "available"},  # Root - should be skipped
            ("properties", "id"): {"selected": True},
            ("properties", "name"): {"inclusion": "automatic"},
            ("properties", "email"): {"selected": False},  # Not selected
            ("properties", "created_at"): {"selected": False},  # Not selected
        }

        # Mock return values
        mock_get_catalog_entry.return_value = {
            "schema": mock_schema,
            "metadata": "dummy_metadata"
        }
        mock_to_map.return_value = mock_metadata_map

        # Call the method
        result = Context.get_unselected_fields("test_stream")

        # Expecting unselected fields
        expected = ["email", "created_at"]
        self.assertCountEqual(result, expected)

    def setUp(self):
        class TestClass(Stream):
            def get_query(self):
                return dedent("""
                    query GetProducts($first: Int!, $after: String, $query: String) {
                        products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                            edges {
                                node {
                                    id
                                    title
                                    descriptionHtml
                                    vendor
                                    category {
                                        id
                                    }
                                    tags
                                    handle
                                    publishedAt
                                    createdAt
                                    updatedAt
                                    templateSuffix
                                    status
                                    productType
                                    giftCardTemplateSuffix
                                    hasOnlyDefaultVariant
                                    hasOutOfStockVariants
                                    hasVariantsThatRequiresComponents
                                    isGiftCard
                                    description
                                    requiresSellingPlan
                                    totalInventory
                                    media(first: 250) {
                                    edges {
                                        node {
                                            id
                                            alt
                                            status
                                            mediaContentType
                                            mediaWarnings {
                                                code
                                                message
                                            }
                                            mediaErrors {
                                                code
                                                details
                                                message
                                            }
                                            ... on ExternalVideo {
                                                id
                                                embedUrl
                                            }
                                            ... on MediaImage {
                                                id
                                                updatedAt
                                                createdAt
                                                mimeType
                                                image {
                                                    url
                                                    width
                                                    height
                                                    id
                                                }
                                            }
                                            ... on Model3d {
                                                id
                                                filename
                                                sources {
                                                    url
                                                    format
                                                    mimeType
                                                    filesize
                                                }
                                            }
                                            ... on Video {
                                                id
                                                updatedAt
                                                createdAt
                                                filename
                                                sources {
                                                    url
                                                    format
                                                    mimeType
                                                    fileSize
                                                }
                                            }
                                        }
                                    }
                                }
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                    """)
        self.instance = TestClass()

    def test_remove_fields_from_query(self):
        result = self.instance.remove_fields_from_query(['requiresSellingPlan', 'totalInventory', 'media'])

        expected_query = dedent("""
                    query GetProducts($first: Int!, $after: String, $query: String) {
                        products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                            edges {
                                node {
                                    id
                                    title
                                    descriptionHtml
                                    vendor
                                    category {
                                        id
                                    }
                                    tags
                                    handle
                                    publishedAt
                                    createdAt
                                    updatedAt
                                    templateSuffix
                                    status
                                    productType
                                    giftCardTemplateSuffix
                                    hasOnlyDefaultVariant
                                    hasOutOfStockVariants
                                    hasVariantsThatRequiresComponents
                                    isGiftCard
                                    description
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                    """).strip()

        # Normalize whitespace for comparison
        self.assertEqual(
            ''.join(result.split()),
            ''.join(expected_query.split())
        )
