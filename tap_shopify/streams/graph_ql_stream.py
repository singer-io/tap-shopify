from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling, HiddenPrints, GraphQLGeneralError, GraphQLThrottledError,
                                      DATE_WINDOW_SIZE, LOGGER)

import datetime
import singer
from singer import metrics, utils
from tap_shopify.context import Context
import shopify
import json
from gql_query_builder import GqlQuery


class GraphQlChildStream(Stream):
    name = None
    replication_key = 'updatedAt'
    replication_object = None
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

        for parent_obj in self.get_children_by_graph_ql(selected_parent):
            if isinstance(parent_obj[self.parent_key_access], dict):
                parent_obj[self.parent_key_access] = [parent_obj[self.parent_key_access]]

            try:
                for child_obj in parent_obj[self.parent_key_access]:
                    child_obj = self.transform_obj(child_obj)
                    child_obj["parentId"] = self.transform_parent_id(parent_obj["id"])
                    yield child_obj
            except Exception as e:
                # None type is not iterable
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

    def get_children_by_graph_ql(self, parent):
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
                                             parent.name,
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

        if "errors" in response:
            errors = response["errors"]
            singer.log_info(errors)
            if errors[0]["extensions"]["code"] == "THROTTLED":
                raise GraphQLThrottledError("THROTTLED", code=429)

        raise GraphQLGeneralError("Failed", code=500)

    def get_graph_query(self,
                        created_at_min,
                        created_at_max,
                        parent_name,
                        after=None):

        input = {
            "first": self.parent_per_page,
            "query": "\"{min_key}:>'{min_val}' AND {max_key}:<'{max_val}'\"".format(
                min_key=self.get_min_replication_key(),
                max_key=self.get_max_replication_key(),
                min_val=created_at_min,
                max_val=created_at_max),
        }
        child_fields = self.get_graph_ql_prop(self.get_child_table_schema())

        if after:
            input["after"] = "\"{}\"".format(after)

        pageInfo = GqlQuery().fields(['hasNextPage', 'hasPreviousPage'], name='pageInfo').generate()

        child_limit_arg = "(first:{})".format(self.child_per_page) if self.node_argument else ''
        child = GqlQuery().fields(child_fields, name='{child}{limit}'.format(child=self.parent_key_access,
                                                                               limit=child_limit_arg)).generate()
        node = GqlQuery().fields(['id', child, "createdAt", "updatedAt"], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()

        generate_query = GqlQuery().query(parent_name, input=input).fields([pageInfo, edges]).generate()

        return "{%s}" % generate_query

    def get_child_table_schema(self):
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
        for prop_name in properties:
            prop_obj = properties[prop_name]
            prop_type = prop_obj["type"]
            if "generated" in prop_type:
                continue
            if 'object' in prop_type:
                if prop_obj["properties"]:
                    ql_field = GqlQuery().fields(self.get_graph_ql_prop(prop_obj), name=prop_name).generate()
                    ql_fields.append(ql_field)
            elif 'array' in prop_type:
                if prop_obj["items"]["properties"]:
                    ql_field = GqlQuery().fields(self.get_graph_ql_prop(prop_obj["items"]), name=prop_name).generate()
                    if prop_name in self.need_edges_cols:
                        node = GqlQuery().fields(self.get_graph_ql_prop(prop_obj["items"]), name='node').generate()
                        edges = GqlQuery().fields([node], "edges").generate()
                        ql_field = GqlQuery().query(prop_name, input={
                            "first": 5
                        }).fields([edges]).generate()

                    ql_fields.append(ql_field)
            else:
                ql_fields.append(prop_name)

        return ql_fields

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
