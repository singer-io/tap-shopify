import unittest
from unittest.mock import patch
import json
from graphql import parse, FieldNode, SelectionSetNode
from pathlib import Path
from tap_shopify.streams.orders import Orders
from tap_shopify.streams.transactions import Transactions
from tap_shopify.streams.inventory_levels import InventoryLevels
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
            data_key = "products"

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

    def test_remove_fields_preserves_nested_same_name_fields(self):
        """Pruning a top-level field must not remove a same-named field that
        appears inside a nested connection (e.g. lineItems.edges.node).

        This is the regression guard for the original customAttributes bug:
        the top-level ``customAttributes`` field on the order record and the
        ``customAttributes`` field inside ``lineItems.edges.node`` share a
        name.  If only the order-level one is unselected it must be removed
        from ``orders.edges.node`` but left intact inside lineItems.
        """
        class OrderLikeStream(Stream):
            data_key = "orders"

            def get_query(self):
                return dedent("""
                    query GetOrders($first: Int!) {
                        orders(first: $first) {
                            edges {
                                node {
                                    id
                                    customAttributes {
                                        key
                                        value
                                    }
                                    lineItems {
                                        edges {
                                            node {
                                                id
                                                customAttributes {
                                                    key
                                                    value
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                """)

        stream = OrderLikeStream()
        result = stream.remove_fields_from_query(["customAttributes"])

        # Top-level customAttributes should be gone; lineItems and its nested
        # customAttributes should remain intact.
        parsed = parse(result)

        def get_selection_names(selection_set):
            return {s.name.value for s in selection_set.selections if isinstance(s, FieldNode)}

        # Navigate: orders → edges → node
        orders_node = parsed.definitions[0].selection_set.selections[0]
        edges_node = orders_node.selection_set.selections[0]
        node_fields = edges_node.selection_set.selections[0]
        top_level_names = get_selection_names(node_fields.selection_set)

        self.assertNotIn(
            "customAttributes", top_level_names,
            "Top-level customAttributes should be pruned from orders.edges.node"
        )
        self.assertIn(
            "lineItems", top_level_names,
            "lineItems should still be present after pruning"
        )

        # Navigate into lineItems → edges → node
        line_items_field = next(
            s for s in node_fields.selection_set.selections
            if isinstance(s, FieldNode) and s.name.value == "lineItems"
        )
        line_item_edges = line_items_field.selection_set.selections[0]
        line_item_node = line_item_edges.selection_set.selections[0]
        line_item_names = get_selection_names(line_item_node.selection_set)

        self.assertIn(
            "customAttributes", line_item_names,
            "customAttributes inside lineItems.edges.node must NOT be pruned"
        )


class TestRemoveFieldsTransactions(unittest.TestCase):
    """remove_fields_from_query correctly prunes fields from the
    ``transactions { }`` direct-list pattern used by the Transactions stream
    (orders.edges.node.transactions { FIELDS }, no nested edges/node wrapper).
    """

    def setUp(self):
        self.stream = Transactions()

    def _parse_transaction_fields(self, query_str):
        """Return the set of field names directly inside ``transactions { }``."""
        parsed = parse(query_str)
        op = parsed.definitions[0]
        # orders → edges → node → transactions
        orders = op.selection_set.selections[0]          # orders
        edges = orders.selection_set.selections[0]       # edges
        node = edges.selection_set.selections[0]         # node
        transactions = next(
            s for s in node.selection_set.selections
            if isinstance(s, FieldNode) and s.name.value == "transactions"
        )
        return {
            s.name.value
            for s in transactions.selection_set.selections
            if isinstance(s, FieldNode)
        }

    def test_unselected_fields_pruned_from_transactions(self):
        """Fields that are unselected should be removed from transactions { }."""
        result = self.stream.remove_fields_from_query(["gateway", "errorCode"])
        remaining = self._parse_transaction_fields(result)

        self.assertNotIn("gateway", remaining)
        self.assertNotIn("errorCode", remaining)

    def test_selected_fields_kept_in_transactions(self):
        """Fields that ARE selected must remain in transactions { }."""
        result = self.stream.remove_fields_from_query(["gateway"])
        remaining = self._parse_transaction_fields(result)

        self.assertIn("id", remaining)
        self.assertIn("status", remaining)
        self.assertIn("createdAt", remaining)

    def test_empty_fields_to_remove_leaves_query_intact(self):
        """Passing an empty list must not alter the transactions fields."""
        original_fields = self._parse_transaction_fields(self.stream.get_query())
        result = self.stream.remove_fields_from_query([])
        pruned_fields = self._parse_transaction_fields(result)

        self.assertEqual(original_fields, pruned_fields)


class TestRemoveFieldsInventoryLevels(unittest.TestCase):
    """remove_fields_from_query correctly targets the inner
    ``inventoryLevels.edges.node { FIELDS }`` selection set and does NOT
    prune from the outer ``locations.edges.node`` selection set.
    """

    def setUp(self):
        self.stream = InventoryLevels()

    def _parse_outer_location_fields(self, query_str):
        """Return field names directly inside ``locations.edges.node { }``."""
        parsed = parse(query_str)
        op = parsed.definitions[0]
        locations = op.selection_set.selections[0]        # locations
        edges = locations.selection_set.selections[0]     # edges
        node = edges.selection_set.selections[0]          # outer node
        return {
            s.name.value
            for s in node.selection_set.selections
            if isinstance(s, FieldNode)
        }

    def _parse_inner_inventory_level_fields(self, query_str):
        """Return field names directly inside ``inventoryLevels.edges.node { }``."""
        parsed = parse(query_str)
        op = parsed.definitions[0]
        locations = op.selection_set.selections[0]
        edges = locations.selection_set.selections[0]
        outer_node = edges.selection_set.selections[0]
        inventory_levels = next(
            s for s in outer_node.selection_set.selections
            if isinstance(s, FieldNode) and s.name.value == "inventoryLevels"
        )
        inner_edges = next(
            s for s in inventory_levels.selection_set.selections
            if isinstance(s, FieldNode) and s.name.value == "edges"
        )
        inner_node = inner_edges.selection_set.selections[0]
        return {
            s.name.value
            for s in inner_node.selection_set.selections
            if isinstance(s, FieldNode)
        }

    def test_unselected_fields_pruned_from_inner_node(self):
        """Unselected inventory-level fields are removed from the inner node."""
        result = self.stream.remove_fields_from_query(["canDeactivate", "deactivationAlert"])
        inner_fields = self._parse_inner_inventory_level_fields(result)

        self.assertNotIn("canDeactivate", inner_fields)
        self.assertNotIn("deactivationAlert", inner_fields)

    def test_outer_location_node_is_not_pruned(self):
        """The outer locations.edges.node must never be touched by pruning."""
        original_outer = self._parse_outer_location_fields(self.stream.get_query())
        result = self.stream.remove_fields_from_query(["canDeactivate", "id", "updatedAt"])
        pruned_outer = self._parse_outer_location_fields(result)

        # The outer node must be unchanged regardless of what fields_to_remove contains.
        self.assertEqual(
            original_outer, pruned_outer,
            "Pruning inventory-level schema fields must not affect the outer "
            "locations.edges.node selection set"
        )

    def test_selected_fields_kept_in_inner_node(self):
        """Fields that ARE selected must remain in the inner inventory-level node."""
        result = self.stream.remove_fields_from_query(["canDeactivate"])
        inner_fields = self._parse_inner_inventory_level_fields(result)

        self.assertIn("id", inner_fields)
        self.assertIn("updatedAt", inner_fields)
        self.assertIn("item", inner_fields)

