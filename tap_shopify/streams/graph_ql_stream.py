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
import typing


class GraphQlStream(Stream):
    name = None
    replication_key = 'updatedAt'
    replication_object = None
    parent_per_page = 100
    need_edges_cols = []
    fragment_cols = {}

    def get_objects(self):
        for obj in self.get_graph_ql_data(self):
            yield self.transform_obj(obj)

    def transform_obj(self, obj: dict) -> dict:
        for col in self.need_edges_cols:
            obj[col] = [trans_obj["node"] for trans_obj in obj[col]["edges"]]
        return obj

    def sync(self):
        for obj in self.get_objects():
            yield obj

    def get_graph_ql_data(self, replication_obj: Stream):
        LOGGER.info("Getting data with GraphQL")
        updated_at_min = replication_obj.get_bookmark()

        today_date = singer.utils.now().replace(microsecond=0)
        stop_time = today_date
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Retrieve data for max 1 year. Otherwise log incremental needed.
        diff_days = (stop_time - updated_at_min).days
        yearly = False
        records = 0
        if diff_days > 365:
            yearly = True
            stop_time = updated_at_min + datetime.timedelta(days=365)
            LOGGER.info("This import will only import the first year of historical data. "
                        "You need to trigger further incremental imports to get the missing rows.")

        max_time = 24
        started_at = datetime.now()
        # Page through till the end of the result set
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
                                             replication_obj.name,
                                             after=after)
                with metrics.http_request_timer(replication_obj.name):
                    data = self.excute_graph_ql(query)
                data = data[replication_obj.name]
                page_info = data['pageInfo']
                edges = data["edges"]
                node = {}

                for edge in edges:
                    after = edge["cursor"]
                    node = edge["node"]
                    records += len(node)
                    yield node

                if not page_info["hasNextPage"]:
                    Context.state.get('bookmarks', {}).get(replication_obj.name, {}).pop('since_id', None)
                    replication_obj.update_bookmark(utils.strftime(updated_at_max + datetime.timedelta(seconds=1)))
                    break

            updated_at_min = updated_at_max + datetime.timedelta(seconds=1)
            # count records and add additional window size time if no data found
            if not records and stop_time < today_date:
                stop_time += datetime.timedelta(days=date_window_size)

            # Check if import start time until now exceeds max allowed hours
            run_hours = (datetime.datetime.now() - started_at).seconds / 3600
            if run_hours > max_time:
                LOGGER.info("Import time of %s hours exceeds allowed max hours %s. "
                            "Please trigger further incremental data to get the missing rows.",
                            int(run_hours), max_time)
                break

        if yearly:
            LOGGER.info("This import only imported one year of historical data. "
                        "Please trigger further incremental data to get the missing rows.")

    @shopify_error_handling
    def excute_graph_ql(self, query: str) -> dict:
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
                        created_at_min: str,
                        created_at_max: str,
                        name: str,
                        after: typing.Optional[str] = None) -> str:

        edges = self.get_graph_edges()

        page_info = GqlQuery().fields(['hasNextPage', 'hasPreviousPage'], name='pageInfo').generate()

        query_input = self.get_query_input(created_at_min, created_at_max, after)
        generate_query = GqlQuery().query(name, input=query_input).fields([page_info, edges]).generate()

        return "{%s}" % generate_query

    def get_graph_edges(self) -> str:
        fields = self.get_graph_ql_prop(self.get_table_schema())

        node = GqlQuery().fields([fields], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges

    def get_query_input(self, created_at_min: str, created_at_max: str, after: typing.Optional[str]) -> dict:
        inputs = {
            "first": self.parent_per_page,
            "query": "\"{min_key}:>'{min_val}' AND {max_key}:<'{max_val}'\"".format(
                min_key=self.get_min_replication_key(),
                max_key=self.get_max_replication_key(),
                min_val=created_at_min,
                max_val=created_at_max),
        }

        if after:
            inputs["after"] = "\"{}\"".format(after)

        return inputs

    def get_table_schema(self) -> dict:
        streams = Context.catalog["streams"]
        schema = None
        for stream in streams:
            if stream["tap_stream_id"] == self.name:
                schema = stream["schema"]
                break

        return schema

    def get_graph_ql_prop(self, schema: dict) -> list:
        properties = schema["properties"]
        ql_fields = []
        for prop_name in properties:
            prop_obj = properties[prop_name]
            prop_type = prop_obj["type"]

            if "generated" in prop_type:
                continue

            if 'object' in prop_type:
                if prop_obj["properties"]:
                    fields = self.get_fragment_fields(prop_name, self.get_graph_ql_prop(prop_obj))
                    ql_field = GqlQuery().fields(fields, name=prop_name).generate()
                    ql_fields.append(ql_field)

            elif 'array' in prop_type:
                if prop_obj["items"]["properties"]:
                    fields = self.get_fragment_fields(prop_name, self.get_graph_ql_prop(prop_obj["items"]))
                    ql_field = GqlQuery().fields(fields, name=prop_name).generate()
                    if prop_name in self.need_edges_cols:
                        node = GqlQuery().fields(fields, name='node').generate()
                        edges = GqlQuery().fields([node], "edges").generate()
                        ql_field = GqlQuery().query(prop_name, input={
                            "first": 5
                        }).fields([edges]).generate()

                    ql_fields.append(ql_field)
            else:
                ql_fields.append(prop_name)

        return ql_fields

    def get_fragment_fields(self, prop_name: str, fields: list) -> list:
        if prop_name in self.fragment_cols:
            fragment_name = self.fragment_cols[prop_name]
            fields = [GqlQuery().fields(fields, name="... on {}".format(fragment_name)).generate()]
        return fields

    def get_extra_query(self):
        return ""

    def get_max_replication_key(self) -> str:
        switch = {
            "createdAt": "created_at",
            "updatedAt": "updated_at"
        }
        return switch[self.replication_key]

    def get_min_replication_key(self) -> str:
        switch = {
            "createdAt": "created_at",
            "updatedAt": "updated_at"
        }
        return switch[self.replication_key]


class GraphQlChildStream(GraphQlStream):
    parent_key_access = None
    parent_name = None
    parent_replication_key = 'updatedAt'
    parent_id_ql_prefix = ''
    child_per_page = 250
    parent_per_page = 100
    child_is_list = True

    def get_objects(self):
        parent = Context.stream_objects[self.parent_name]()
        parent.replication_key = self.parent_replication_key
        for parent_obj in self.get_graph_ql_data(parent):

            if not parent_obj[self.parent_key_access]:
                continue

            if isinstance(parent_obj[self.parent_key_access], dict):
                child_obj = self.transform_obj(parent_obj[self.parent_key_access])
                child_obj["parentId"] = self.transform_parent_id(parent_obj["id"])
                yield child_obj
                continue

            for child_obj in parent_obj[self.parent_key_access]:
                child_obj = self.transform_obj(child_obj)
                child_obj["parentId"] = self.transform_parent_id(parent_obj["id"])

                yield child_obj

    def transform_parent_id(self, parent_id: str) -> str:
        parent_id = parent_id.replace(self.parent_id_ql_prefix, '')
        return parent_id

    def get_graph_edges(self) -> str:
        child_fields = self.get_graph_ql_prop(self.get_table_schema())
        child_limit_arg = "(first:{})".format(self.child_per_page) if self.child_is_list else ''
        child = GqlQuery().fields(child_fields, name='{child}{limit}'.format(child=self.parent_key_access,
                                                                             limit=child_limit_arg)).generate()
        node = GqlQuery().fields(['id', child, "createdAt", "updatedAt"], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges
