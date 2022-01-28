import json
import shopify
import singer
import typing
import datetime
import time
from singer import metrics, utils
from gql_query_builder import GqlQuery
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling, HiddenPrints, GraphQLGeneralError,
                                      LOGGER, GraphQLRestoreRateError)
from tap_shopify.streams.graph_ql_stream import (GraphQlChildStream)

LOGGER = singer.get_logger()
Context.stream_objects['collections'] = Context.stream_objects['custom_collections']
Context.stream_objects['collections'].name = 'collections'


def get_selected_parents():
    for parent_stream in ['orders', 'customers', 'products', 'collections']:
        yield Context.stream_objects[parent_stream]()


class MetafieldsQL(GraphQlChildStream):
    name = 'metafields_ql'
    replication_object = shopify.Metafield
    parent_key_access = "metafields"
    child_per_page = 20
    parent_per_page = 25

    def transform_obj(self, obj: dict) -> dict:
        if self.need_edges_cols and obj.get("edges", False):
            for col in self.need_edges_cols:
                obj[col] = [trans_obj["node"] for trans_obj in obj["edges"]]
        if obj.get("node", False):
            obj = obj["node"]
        return obj

    def get_query_input(self, created_at_min: str, created_at_max: str, after: typing.Optional[str]) -> dict:
        inputs = {
            "first": self.parent_per_page,
            "query": "\"{min_key}:>'{min_val}'\"".format(
                min_key=self.get_min_replication_key(),
                min_val=created_at_min),
            "sortKey": 'UPDATED_AT'
        }
        # parent pagination
        if after:
            inputs["after"] = "\"{}\"".format(after)
        return inputs

    def get_graph_query(self,
                        created_at_min: str,
                        created_at_max: str,
                        name: str,
                        after: typing.Optional[str] = None,
                        pagination: typing.Optional[str] = None) -> str:

        edges = self.get_graph_edges(pagination)  # child pagination
        page_info = GqlQuery().fields(['hasNextPage', 'hasPreviousPage'], name='pageInfo').generate()
        query_input = self.get_query_input(created_at_min, created_at_max, after)
        generate_query = GqlQuery().query(name, input=query_input).fields([page_info, edges]).generate()

        return "{%s}" % generate_query

    def get_graph_edges(self, pagination: typing.Optional[str]) -> str:
        child_fields = self.get_graph_ql_prop(self.get_table_schema())
        child_node = GqlQuery().fields(child_fields, name='node').generate()
        child_edges = GqlQuery().fields(['cursor', child_node], name='edges').generate()

        child_per_page = self.child_per_page
        if pagination:
            child_per_page = '{}, after: "{}"'.format(self.child_per_page, pagination)
        child_limit_arg = "(first:{})".format(child_per_page) if self.child_is_list else ''
        name ='{child}{limit}'.format(child=self.parent_key_access, limit=child_limit_arg)
        page_info = GqlQuery().fields(['hasNextPage', 'hasPreviousPage'], name='pageInfo').generate()
        child = GqlQuery().fields([page_info, child_edges], name).generate()
        node = GqlQuery().fields(['id', child, "updatedAt"], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges

    def get_objects(self):
        for selected_parent in get_selected_parents():
            parent = Context.stream_objects[selected_parent.name]()
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

    def get_graph_ql_data(self, replication_obj: Stream):
        LOGGER.info("Getting data with GraphQL")
        updated_at_min = replication_obj.get_bookmark()
        stream_name = replication_obj.name + "_metafields"
        updated_at = (singer.get_bookmark(Context.state, stream_name,  replication_obj.replication_key)
                      or Context.config["start_date"])
        parent_pagination = None
        child_pagination = None
        record_date = None
        while True:
            if record_date and abs((updated_at_min.date() - record_date)).days > 365:
                LOGGER.info("This import only imported one year of historical data. "
                            "Please trigger further incremental data to get the missing rows.")

                Context.state.get('bookmarks', {}).get(stream_name, {}).pop('since_id', None)
                if node.get("updatedAt", False):
                    updated = utils.strftime(utils.strptime_with_tz(node["updatedAt"])
                                             + datetime.timedelta(seconds=1))
                    replication_obj.name = stream_name
                    replication_obj.update_bookmark(updated)
                break

            query = self.get_graph_query(updated_at,
                                         None,
                                         replication_obj.name,
                                         after=parent_pagination,
                                         pagination=child_pagination)
            with metrics.http_request_timer(replication_obj.name):
                data = self.excute_graph_ql(query)
            data = data[replication_obj.name]
            page_info = data['pageInfo']
            edges = data["edges"]

            for edge in edges:
                parent_pagination = edge["cursor"]
                node = edge["node"]
                pagination = node[self.parent_key_access]["pageInfo"]["hasNextPage"]
                # update node with children
                node[self.parent_key_access] = node[self.parent_key_access]["edges"]

                # child pagination
                if pagination:
                    for child_edge in node[self.parent_key_access]:
                        child_pagination = child_edge["cursor"]
                        yield {
                            'id': node['id'],
                            self.parent_key_access: child_edge,
                            'updatedAt': node['updatedAt']
                        }

                if node.get("updatedAt", False) and utils.strptime_with_tz(node.get("updatedAt")).date() != record_date:
                    record_date = utils.strptime_with_tz(node.get("updatedAt")).date()
                    LOGGER.info("Imorted till {} of updated {}".format(record_date, replication_obj.name))
                yield node

            if not page_info["hasNextPage"]:
                Context.state.get('bookmarks', {}).get(stream_name, {}).pop('since_id', None)
                if node.get("updatedAt", False):
                    updated = utils.strftime(utils.strptime_with_tz(node["updatedAt"])
                                             + datetime.timedelta(seconds=1))
                    replication_obj.name = stream_name
                    replication_obj.update_bookmark(updated)
                break

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
                # calculate sleep time on restore rate in response, than raise throttled
                if response.get("extensions", False):
                    cost = response["extensions"].get("cost")
                    requested_query_cost = cost.get("requestedQueryCost", 0)
                    currently_available = cost["throttleStatus"].get("currentlyAvailable", 0)
                    restore_rate = cost["throttleStatus"].get("restoreRate", 0)
                    # restore rate per second
                    seconds_to_sleep = (requested_query_cost - currently_available) / restore_rate
                    LOGGER.info("Requested query cost is {}, but available is only {} with restore rate {}/s".format(
                        requested_query_cost, currently_available, restore_rate))
                    if seconds_to_sleep > 1:
                        LOGGER.info("Will sleep {} seconds".format(seconds_to_sleep))
                        time.sleep(seconds_to_sleep - 1)
                        # +1 second will sleep before retry
                    raise GraphQLRestoreRateError("RESTORE-RATE", code=429)
                    # raise GraphQLThrottledError("THROTTLED", code=429)

        raise GraphQLGeneralError("Failed", code=500)


Context.stream_objects['metafields_ql'] = MetafieldsQL

