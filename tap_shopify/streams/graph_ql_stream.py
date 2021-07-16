from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling, HiddenPrints, GraphQLGeneralError, GraphQLThrottledError,
                                      DATE_WINDOW_SIZE, LOGGER)

import datetime
import singer
from singer import metrics, utils
from tap_shopify.context import Context
import shopify
import json


class GraphQlChildStream(Stream):
    name = None
    replication_key = 'updatedAt'
    replication_object = shopify.Transaction
    parent_key_access = None
    parent_name = None
    parent_replication_key = 'updatedAt'
    parent_id_ql_prefix = ''
    node_argument = True
    child_per_page = 250
    parent_per_page = 100
    need_edges_cols = []

    def get_objects(self):
        selected_parent = Context.stream_objects[self.parent_name]()
        selected_parent.replication_key = self.parent_replication_key

        schema = self.get_table_schema()
        ql_properties = self.get_graph_ql_prop(schema)
        for parent_obj in self.get_children_by_graph_ql(selected_parent, self.parent_key_access, ql_properties):
            if isinstance(parent_obj[self.parent_key_access], dict):
                parent_obj[self.parent_key_access] = [parent_obj[self.parent_key_access]]
            try:
                for child_obj in parent_obj[self.parent_key_access]:
                    child_obj = self.transform_obj(child_obj)
                    child_obj["parentId"] = self.transform_parent_id(parent_obj["id"])
                    yield child_obj
            except Exception as e:
                # print("Exception raised: ", e)
                pass

    def transform_obj(self, obj):
        for col in self.need_edges_cols:
            obj[col] = [trans_obj["node"] for trans_obj in obj[col]["edges"]]
        return obj

    def sync(self):
        for child_obj in self.get_objects():
            yield child_obj

    def transform_parent_id(self, parent_id):
        parent_id = parent_id.replace(self.parent_id_ql_prefix, '')
        return parent_id

    def get_children_by_graph_ql(self, parent, child, child_parameters):
        LOGGER.info("Getting data with GraphQL")

        updated_at_min = parent.get_bookmark()

        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Page through till the end of the resultset
        while updated_at_min < stop_time:
            after = None
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)

            if updated_at_max > stop_time:
                updated_at_max = stop_time
            singer.log_info("getting from %s - %s", updated_at_min,
                            updated_at_max)
            while True:
                query = self.get_graph_query(updated_at_min,
                                             updated_at_max,
                                             self.parent_per_page,
                                             child,
                                             child_parameters,
                                             parent.name,
                                             self.child_per_page,
                                             self.node_argument,
                                             after=after)
                with metrics.http_request_timer(parent.name):
                    data = self.excute_graph_ql(query)
                data = data[parent.name]
                page_info = data['pageInfo']
                edges = data["edges"]
                for edge in edges:
                    after = edge["cursor"]
                    node = edge["node"]
                    yield node
                if not page_info["hasNextPage"]:
                    Context.state.get('bookmarks', {}).get(parent.name, {}).pop('since_id', None)
                    parent.update_bookmark(utils.strftime(updated_at_max + datetime.timedelta(seconds=1)))
                    break

            updated_at_min = updated_at_max + datetime.timedelta(seconds=1)

    @shopify_error_handling
    def excute_graph_ql(self, query):
        try:
            # the execute function sometimes prints and this causes errors for the target, so I block printing for it
            with HiddenPrints():
                response = json.loads(shopify.GraphQL().execute(query))
        except Exception:
            raise GraphQLGeneralError("Execution failed", code=500)
        if 'data' in response and response['data'] is not None:
            return response['data']
        else:
            if "errors" in response:
                errors = response["errors"]
                singer.log_info(errors)
                if errors[0]["extensions"]["code"] == "THROTTLED":
                    raise GraphQLThrottledError("THROTTLED", code=429)

            raise GraphQLGeneralError("Failed", code=500)

    def get_graph_query(self, created_at_min, created_at_max, limit, child, child_parameters, parent_name,
                        child_limit=100, node_argument=True,
                        after=None):
        argument = ''
        if node_argument:
            argument = "(first:%i)" % (
                child_limit)
        query = """{
                      %s(first:%i %s ,query:"%s:>'%s' AND %s:<'%s' %s") {
                        pageInfo { # Returns details about the current page of results
                          hasNextPage # Whether there are more results after this page
                          hasPreviousPage # Whether there are more results before this page
                        }
                        edges{
                          cursor
                          node{
                            id,
                            %s """ + argument + """{
                              %s
                            }
                            createdAt
                          }
                        }
                      }
                }"""
        after_str = ''
        if after:
            after_str = ',after:"%s"' % after
        query = query % (
            parent_name,
            limit,
            after_str,
            self.get_min_replication_key(),
            created_at_min,
            self.get_max_replication_key(),
            created_at_max,
            self.get_extra_query(),
            child,
            child_parameters)
        return query

    def get_table_schema(self):
        streams = Context.catalog["streams"]
        schema = None
        for stream in streams:
            if stream["tap_stream_id"] == self.name:
                schema = stream["schema"]
                break

        return schema

    def get_graph_ql_prop(self, schema):
        properties = schema["properties"]
        ql_fields = []
        for prop in properties:
            if "generated" in properties[prop]["type"]:
                continue
            if 'object' in properties[prop]['type']:
                if properties[prop]["properties"]:
                    ql_field = "%s{%s}" % (prop, self.get_graph_ql_prop(properties[prop]))
                    ql_fields.append(ql_field)
            elif 'array' in properties[prop]['type']:
                if properties[prop]["items"]["properties"]:
                    ql_append = "%s{%s}"
                    if prop in self.need_edges_cols:
                        ql_append = "%s(first:5){edges{node{%s}}}"
                    ql_field = ql_append % (
                        prop, self.get_graph_ql_prop(properties[prop]["items"]))
                    ql_fields.append(ql_field)
            else:
                ql_fields.append(prop)
        return ','.join(ql_fields)

    def get_extra_query(self):
        return ""

    def get_max_replication_key(self):
        switch = {
            "createdAt": "created_at",
            "updatedAt": "updated_at"
        }
        return switch[self.replication_key]

    def get_min_replication_key(self):
        switch = {
            "createdAt": "created_at",
            "updatedAt": "updated_at"
        }
        return switch[self.replication_key]
