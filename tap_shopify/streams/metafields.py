import json
import singer
import datetime
import shopify

from singer import utils, metrics

from tap_shopify.context import Context
from tap_shopify.streams.graphql import (
    get_parent_ids,
    get_metadata_query,
    get_metadata_query_customers,
    get_metadata_query_product,
    get_metadata_query_collection,
    get_metadata_query_order,


)
from tap_shopify.streams.graphql.gql_base import (
    ShopifyGqlStream, shopify_error_handling, ShopifyGraphQLError
    )


LOGGER = singer.get_logger()



class Metafields(ShopifyGqlStream):
    name = 'metafields'
    data_key = "metafields"
    replication_key = "updated_at"

    selected_parent = None

    parent_alias = {
        "custom_collections":"collections"
    }

    resource_alias = {
        "customers":"customer",
        "products":"product",
        "collections": "collection"
    }
    # pylint: disable=W0221
    def get_query(self, data_key):
        return get_metadata_query(self, data_key)

    @shopify_error_handling
    def call_api(self, query_params, query, data_key):
        response = shopify.GraphQL().execute(query=query, variables=query_params)
        response = json.loads(response)
        if "errors" in response.keys():
            raise ShopifyGraphQLError(response['errors'])
        data = response.get("data", {}).get(data_key, {})
        return data


    def get_selected_parents(self):
        for parent_stream in ['orders', 'customers', 'products', 'custom_collections']:
            if Context.is_selected(parent_stream):
                yield Context.stream_objects[parent_stream]()

    def get_parents(self):

        for parent in self.get_selected_parents():
            parent = self.parent_alias.get(parent, parent)
            LOGGER.info("Fetching id's for %s", parent)

            updated_at_min = self.get_bookmark()
            stop_time = utils.now().replace(microsecond=0)
            date_window_size = 30

            while updated_at_min < stop_time:
                updated_at_max = min(updated_at_min + datetime.timedelta(days=date_window_size),stop_time)
                has_next_page, cursor = True, None

                while has_next_page:
                    query_params = self.get_query_params(updated_at_min, updated_at_max, cursor)
                    query = get_parent_ids(self, parent)

                    with metrics.http_request_timer(self.name):
                        data = self.call_api(query_params, query, parent)

                    for edge in data.get("edges"):
                        obj = edge.get("node")
                        resource_alias = self.resource_alias.get(parent, parent)
                        yield (obj, resource_alias)

                    page_info =  data.get("pageInfo")
                    cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")
                updated_at_min = updated_at_max
        parent = None

    def get_objects(self):
        for parent_obj, resource_type in self.get_parents():
            if resource_type == "customer":
                 query = get_metadata_query_customers()
            elif resource_type == "product":
                query = get_metadata_query_product()
            elif resource_type == "collection":
                query = get_metadata_query_collection()
            elif resource_type == "order":
                query = get_metadata_query_order()
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
                    yield obj
                page_info =  data.get("pageInfo")
                cursor,has_next_page = page_info.get("endCursor"),page_info.get("hasNextPage")


    def transform_object(self, obj):
        obj["id"] = int(obj["id"].replace("gid://shopify/Metafield/", ""))
        obj["value_type"] = obj["type"] or None
        if val_type in ["json", "weight", "volume", "dimension", "rating"]:
            value = metafield.get("value")


        return obj


    def sync(self):

        for metafield in self.get_objects():
            metafield = self.transform_object(metafield)
            val_type = metafield["value_type"]
            if val_type in ["json", "weight", "volume", "dimension", "rating"]:
                value = metafield.get("value")
                try:
                    metafield["value"] = json.loads(value) if value is not None else value
                except json.decoder.JSONDecodeError:
                    LOGGER.info("Failed to decode JSON value for metafield %s", metafield.get('id'))
                    metafield["value"] = value

            yield metafield

Context.stream_objects['metafields'] = Metafields
