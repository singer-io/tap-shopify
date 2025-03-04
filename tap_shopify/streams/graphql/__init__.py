"""
tap shopify graphql module
"""

from .gql_base import ShopifyGqlStream, ShopifyGraphQLError

from .gql_queries import (
    get_metafields_query,
    get_inventory_items_query,
    get_metafield_query_collection,
    get_metafield_query_order,
    get_metafield_query_customers,
    get_parent_ids_query,
    get_product_variant_query,
    get_metafield_query_product,
    get_metafield_query_shop,
    get_products_query,
    get_customers_query,
    get_events_query,
    get_inventory_levels_query,
    get_locations_query,
    get_order_refunds_query,
    get_orders_query,
    get_transactions_query)
