import shopify
from tap_shopify.context import Context
from tap_shopify.streams.graph_ql_stream import GraphQlChildStream
from gql_query_builder import GqlQuery
import typing
import singer
from singer import metrics, utils
import datetime, time
import json
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling, HiddenPrints, GraphQLGeneralError, GraphQLThrottledError,
                                      LOGGER, GraphQLRestoreRateError)

Context.stream_objects['priceRules'] = Context.stream_objects['price_rules']
Context.stream_objects['priceRules'].name = 'priceRules'


class DiscountCodesQL(GraphQlChildStream):
    name = 'discount_codes_ql'

    replication_object = shopify.DiscountCode
    parent_key_access = "discountCodes"
    replication_key = 'createdAt'
    parent_name = "priceRules"
    parent_id_ql_prefix = 'gid://shopify/PriceRule/'
    child_per_page = 20
    parent_per_page = 25
    need_edges_cols = ["discountCodes"]

    def transform_obj(self, obj: dict) -> dict:
        if self.need_edges_cols and obj.get("edges", False):
            for col in self.need_edges_cols:
                obj[col] = [trans_obj["node"] for trans_obj in obj["edges"]]
        if obj.get("node", False):
            obj = obj["node"]
        return obj

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
        node = GqlQuery().fields(['id', child, "createdAt"], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges

    def get_query_input(self, created_at_min: str, created_at_max: str, after: typing.Optional[str]) -> dict:
        inputs = {
            "first": self.parent_per_page,
            "query": "\"{min_key}:>'{min_val}'\"".format(
                min_key=self.get_min_replication_key(),
                min_val=created_at_min),
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

    def get_graph_ql_data(self, replication_obj: Stream):
        LOGGER.info("Getting data with GraphQL")
        updated_at_min = replication_obj.get_bookmark()
        created_at = (singer.get_bookmark(Context.state, replication_obj.name,  replication_obj.replication_key)
                      or Context.config["start_date"])
        parent_pagination = None
        child_pagination = None
        record_date = None
        while True:
            if record_date and abs((updated_at_min.date() - record_date)).days > 365:
                LOGGER.info("This import only imported one year of historical data. "
                            "Please trigger further incremental data to get the missing rows.")

                Context.state.get('bookmarks', {}).get(replication_obj.name, {}).pop('since_id', None)
                if node.get("createdAt", False):
                    created = utils.strftime(utils.strptime_with_tz(node["createdAt"])
                                             + datetime.timedelta(seconds=1))
                    replication_obj.update_bookmark(created)
                break

            query = self.get_graph_query(created_at,
                                         None,
                                         replication_obj.name,
                                         after=parent_pagination,
                                         pagination=child_pagination)
            with metrics.http_request_timer(replication_obj.name):
                data = self.excute_graph_ql(query)
            data = data[replication_obj.name]
            page_info = data['pageInfo']
            edges = data["edges"]
            node = {}

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
                            'createdAt': node['createdAt']
                        }

                if node.get("createdAt", False) and utils.strptime_with_tz(node.get("createdAt")).date() != record_date:
                    record_date = utils.strptime_with_tz(node.get("createdAt")).date()
                    LOGGER.info("Imorted till {} of created price rules".format(record_date))
                yield node

            if not page_info["hasNextPage"]:
                # Price rule created date is used as state
                Context.state.get('bookmarks', {}).get(replication_obj.name, {}).pop('since_id', None)
                if node.get("createdAt", False):
                    created = utils.strftime(utils.strptime_with_tz(node["createdAt"])
                                             + datetime.timedelta(seconds=1))
                    replication_obj.update_bookmark(created)
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


Context.stream_objects['discount_codes_ql'] = DiscountCodesQL
Context.stream_objects['priceRules'] = Context.stream_objects['price_rules']
