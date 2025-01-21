from datetime import timedelta
import json
import singer
import shopify

from singer import utils, metrics

from tap_shopify.context import Context
from tap_shopify.streams.graphql import (
    get_parent_ids,
    get_metafield_query_customers,
    get_metafield_query_product,
    get_metafield_query_collection,
    get_metafield_query_order,
)
from tap_shopify.streams.graphql.gql_base import (
    ShopifyGqlStream, shopify_error_handling, ShopifyGraphQLError
    )


LOGGER = singer.get_logger()



class Metafields(ShopifyGqlStream):
    name = 'metafields'
    data_key = "metafields"
    replication_key = "updatedAt"

    selected_parent = None

    parent_alias = {
        "custom_collections":"collections"
    }

    # required to access correct key from graphql response
    # maps object list to single object access key
    resource_alias = {
        "customers":"customer",
        "products":"product",
        "collections": "collection",
        "orders": "order"
    }
    # pylint: disable=W0221
    def get_query(self):
        return None

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor,):
        """
        Returns Query and pagination params for filtering
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
            "order": get_metafield_query_order
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
        for parent in ['orders', 'customers', 'products', 'custom_collections']:
            parent = self.parent_alias.get(parent, parent)
            LOGGER.info("Fetching id's for %s", parent)

            # To force get all parents from the start date using a blank replication key
            self.name = 'metafield_parents'
            updated_at_min = self.get_bookmark()
            stop_time = utils.now().replace(microsecond=0)
            date_window_size = 30

            while updated_at_min < stop_time:
                updated_at_max = min(\
                    updated_at_min + timedelta(days=date_window_size) , stop_time)
                has_next_page, cursor = True, None

                while has_next_page:
                    query_params = self.get_query_params(\
                        updated_at_min, updated_at_max, cursor)
                    query = get_parent_ids(parent)

                    with metrics.http_request_timer(self.name):
                        data = self.call_api(query_params, query, parent)

                    for edge in data.get("edges"):
                        obj = edge.get("node")
                        resource_alias = self.resource_alias.get(parent, parent)
                        yield (obj, resource_alias)

                    page_info =  data.get("pageInfo")
                    cursor = page_info.get("endCursor")
                    has_next_page = page_info.get("hasNextPage")

                updated_at_min = updated_at_max
        parent = None

    def get_objects(self):

        for parent_obj, resource_type in self.get_parents():
            qury_fnc = self.get_resource_type_query(resource_type)

            if qury_fnc:
                query = qury_fnc()
            else:
                raise ShopifyGraphQLError("Invalid Resource Type")

            has_next_page, cursor = True, None
            query_params = {
                "first": self.results_per_page,
            }

            while has_next_page:
                query_params["pk_id"] = parent_obj["id"]
                if cursor:
                    query_params["cursor"] = cursor
                with metrics.http_request_timer(self.name):
                    response = self.call_api(query_params, query, resource_type)
                data = (response.get("metafields") or {})
                for edge in data.get("edges"):
                    obj = edge.get("node")
                    obj = self.transform_object(obj)
                    yield obj
                page_info =  data.get("pageInfo")
                cursor,has_next_page = page_info.get("endCursor"),page_info.get("hasNextPage")

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
        last_bookmark = self.get_bookmark()
        start_time = utils.now().replace(microsecond=0)
        max_bookmark = last_bookmark

        for obj in self.get_objects():
            replication_value = utils.strptime_to_utc(obj[self.replication_key])
            if replication_value >= last_bookmark:
                max_bookmark = max(replication_value, max_bookmark)
                yield obj

        self.name = 'metafields'
        max_bookmark = min(max_bookmark, start_time)
        self.update_bookmark(utils.strftime(max_bookmark))

Context.stream_objects['metafields'] = Metafields
