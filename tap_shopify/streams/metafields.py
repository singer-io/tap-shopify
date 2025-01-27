from datetime import timedelta
import json
import shopify

from singer import utils, get_logger

from tap_shopify.context import Context
from tap_shopify.streams.graphql import (
    get_parent_ids_query,
    get_metafield_query_customers,
    get_metafield_query_product,
    get_metafield_query_collection,
    get_metafield_query_order,
    get_metafield_query_shop,
)
from tap_shopify.streams.graphql.gql_base import (
    ShopifyGqlStream,
    ShopifyGraphQLError,
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
    def call_api(self, query_params, query, data_key):
        response = shopify.GraphQL().execute(query=query, variables=query_params)
        response = json.loads(response)
        if "errors" in response.keys():
            raise ShopifyGraphQLError(response['errors'])
        data = response.get("data", {}).get(data_key, {})
        return data

    def get_parents(self):
        sync_start = utils.now().replace(microsecond=0)
        for parent in ['shop','orders', 'customers', 'products', 'custom_collections']:
            parent = self.parent_alias.get(parent, parent)
            resource_alias = self.resource_alias.get(parent, parent)
            LOGGER.info("Fetching id's for %s %s", parent, resource_alias)

            if parent == "shop":
                yield None, resource_alias
                continue

            last_updated_at = self.get_bookmark_by_name(f'{self.name}_{resource_alias}')
            date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

            while last_updated_at < sync_start:

                date_window_end = last_updated_at + timedelta(days=date_window_size)
                query_end = min(sync_start, date_window_end)
                has_next_page, cursor = True, None

                while has_next_page:
                    query_params = self.get_query_params(last_updated_at, query_end, cursor)
                    query = get_parent_ids_query(parent)
                    data = self.call_api(query_params, query, parent)
                    for edge in data.get("edges"):
                        yield (edge.get("node"), resource_alias)

                    page_info =  data.get("pageInfo")
                    cursor = page_info.get("endCursor")
                    has_next_page = page_info.get("hasNextPage")

                last_updated_at = query_end

    def get_objects(self):

        for parent_obj, resource_type in self.get_parents():
            qury_fnc = self.get_resource_type_query(resource_type)

            if qury_fnc:
                query = qury_fnc()
            else:
                raise ShopifyGraphQLError("Invalid Resource Type")

            has_next_page, cursor = True, None
            query_params = {"first": self.results_per_page}
            if resource_type != "shop":
                query_params["pk_id"] = parent_obj["id"]

            while has_next_page:
                if cursor:
                    query_params["after"] = cursor
                response = self.call_api(query_params, query, resource_type)
                data = (response.get("metafields") or {})

                for edge in data.get("edges"):
                    obj = self.transform_object(edge.get("node"))
                    yield (obj, resource_type)

                page_info =  data.get("pageInfo")
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

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

    def sync(self):
        start_time = utils.now().replace(microsecond=0)
        current_bookmarks = {}
        last_bookmarks = {}

        for obj, resource_type in self.get_objects():
            replication_value = utils.strptime_to_utc(obj[self.replication_key])

            if resource_type not in current_bookmarks or resource_type not in last_bookmarks:
                bookmark = self.get_bookmark_by_name(f"{self.name}_{resource_type}")
                last_bookmarks[resource_type] = bookmark
                current_bookmarks[resource_type] = bookmark

            if replication_value >= last_bookmarks[resource_type]:
                current_bookmarks[resource_type] = max(replication_value, \
                                                       current_bookmarks[resource_type])
                yield obj

        for res, bookmark_val in current_bookmarks.items():
            bookmark_val = min(start_time, bookmark_val)
            self.update_bookmark(utils.strftime(bookmark_val), f"{self.name}_{res}")

Context.stream_objects['metafields'] = Metafields
