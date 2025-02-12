from datetime import timedelta
import json
import shopify

from singer import utils, get_logger

from tap_shopify.context import Context
from tap_shopify.streams.graphql import (
    get_metafields_query,
    get_metafield_query_customers,
    get_metafield_query_product,
    get_metafield_query_collection,
    get_metafield_query_order,
    get_metafield_query_shop,
)
from tap_shopify.streams.graphql.gql_base import (
    ShopifyGqlStream,
    ShopifyGraphQLError,
    ShopifyAPIError,
    DATE_WINDOW_SIZE,
    shopify_error_handling,
    )

LOGGER = get_logger()

class Metafields(ShopifyGqlStream):
    name = 'metafields'
    data_key = "metafields"
    replication_key = "updatedAt"

    selected_parent = None

    parent_alias = {
        "custom_collections":"collections"
    }


    # maps object list identifier to single object access identifier
    # eg customers -> customer
    resource_alias = {
        "shop": "shop",
        "customers":"customer",
        "products":"product",
        "collections": "collection",
        "orders": "order"
    }

    def get_query(self):
        return None

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        rkey = "updated_at"
        params = {
            "query": f"{rkey}:>='{updated_at_min}' AND {rkey}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def get_resource_type_query(self, resource):
        return {
            "customer": get_metafield_query_customers,
            "product": get_metafield_query_product,
            "collection": get_metafield_query_collection,
            "order": get_metafield_query_order,
            "shop":get_metafield_query_shop
            }.get(resource)

    @shopify_error_handling
    def call_api(self, query_params, query):
        response = shopify.GraphQL().execute(query=query, variables=query_params)
        response = json.loads(response)
        if "errors" in response.keys():
            raise ShopifyAPIError(response['errors'])
        data = response.get("data", {})
        return data


    def transform_object(self, obj):
        obj["value_type"] = obj["type"] or None
        obj["updated_at"] = obj["updatedAt"]
        if obj["value_type"] in ["json", "weight", "volume", "dimension", "rating"]:
            value = obj.get("value")
            try:
                obj["value"] = json.loads(value) if value is not None else value
            except json.decoder.JSONDecodeError:
                LOGGER.info("Failed to decode JSON value for obj %s", obj.get('id'))
                obj["value"] = value
        return obj

    def get_next_page_metafields(self, metafields, query_params, gql_resource):
        id = metafields.get("edges", [])[0]["node"]["owner"]["id"]
        query = self.get_resource_type_query(gql_resource)()
        has_next_page = True
        cursor = metafields.get("pageInfo", {}).get("endCursor")
        while has_next_page:
            query_params["after"] = cursor
            query_params["pk_id"] = id
            data = self.call_api(query_params, query).get(gql_resource, {})
            metafields = data.get("metafields", {})
            for edge in metafields.get("edges", []):
                yield edge
            page_info = metafields.get("pageInfo", {})
            cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage  ", False)

    def get_objects(self):
        """Main iterator to yield metafield objects"""
        sync_start = utils.now().replace(microsecond=0)

        for resource_type, gql_resource in self.resource_alias.items():
            LOGGER.info(f"Syncing metafields for {resource_type}")

            # Handle shop metafields separately
            if resource_type == 'shop':
                query = get_metafields_query(gql_resource)
                has_next_page = True
                cursor = None

                while has_next_page:
                    params = {"first": self.results_per_page}
                    if cursor:
                        params["after"] = cursor

                    data = self.call_api(params, query)
                    metafields = data.get("shop", {}).get("metafields", {})

                    for edge in metafields.get("edges", []):
                        yield self.transform_object(edge["node"])

                    page_info = metafields.get("pageInfo", {})
                    cursor = page_info.get("endCursor")
                    has_next_page = page_info.get("hasNextPage", False)
                continue

            # Handle other resources
            last_updated_at = self.get_bookmark_by_name(f'{self.name}_{resource_type}')
            date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

            while last_updated_at < sync_start:
                date_window_end = last_updated_at + timedelta(days=date_window_size)
                query_end = min(sync_start, date_window_end)

                has_next_page = True
                cursor = None

                while has_next_page:
                    query_params = self.get_query_params(last_updated_at, query_end, cursor)
                    query = get_metafields_query(resource_type)
                    data = self.call_api(query_params, query)

                    resource_data = data.get(resource_type, {})
                    for data_edge in resource_data.get("edges", []):
                        metafields = data_edge["node"].get("metafields", {})
                        for edge in metafields.get("edges", []):
                            yield self.transform_object(edge["node"])
                        meta_page_info = metafields.get("pageInfo", {})
                        meta_has_next_page = meta_page_info.get("hasNextPage", False)
                        if meta_has_next_page:
                            for edge in self.get_next_page_metafields(metafields, query_params, gql_resource):
                                yield self.transform_object(edge["node"])

                    page_info = resource_data.get("pageInfo", {})
                    cursor = page_info.get("endCursor")
                    has_next_page = page_info.get("hasNextPage", False)
                    if has_next_page:
                        pass

                last_updated_at = query_end

    def sync(self):
        """Sync metafields and update bookmarks"""
        start_time = utils.now().replace(microsecond=0)
        current_bookmarks = {}

        for obj in self.get_objects():
            resource_type = obj["ownerType"].lower()
            replication_value = utils.strptime_to_utc(obj["updated_at"])
            current_bookmark_value = self.get_bookmark_by_name(f"{self.name}_{resource_type}")
            if resource_type not in current_bookmarks:
                current_bookmarks[resource_type] = self.get_bookmark_by_name(f"{self.name}_{resource_type}")

            if replication_value >= current_bookmarks[resource_type]:
                current_bookmarks[resource_type] = max(
                    replication_value,
                    current_bookmarks[resource_type]
                )
            if replication_value >= current_bookmark_value:
                yield obj

        # Update bookmarks
        for res, bookmark_val in current_bookmarks.items():
            bookmark_val = min(start_time, bookmark_val)
            self.update_bookmark(utils.strftime(bookmark_val), f"{self.name}_{res}")

Context.stream_objects['metafields'] = Metafields
