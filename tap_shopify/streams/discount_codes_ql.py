import shopify
from tap_shopify.context import Context
from tap_shopify.streams.graph_ql_stream import GraphQlChildStream
from gql_query_builder import GqlQuery
import typing


class DiscountCodes(GraphQlChildStream):
    name = 'discount_codes_ql'

    replication_object = shopify.DiscountCode
    parent_key_access = "discountCodes"
    parent_name = "priceRules"
    parent_id_ql_prefix = 'gid://shopify/PriceRule/'
    child_per_page = 25
    parent_per_page = 25

    def transform_obj(self, obj):
        if "edges" in obj:
            try:
                obj = obj["edges"][0]["node"]
            except:
                pass
        return obj

    def get_graph_edges(self) -> str:
        child_fields = self.get_graph_ql_prop(self.get_table_schema())
        child_node = GqlQuery().fields(child_fields, name='node').generate()
        child_edges = GqlQuery().fields([child_node], name='edges').generate()

        child_limit_arg = "(first:{})".format(self.child_per_page) if self.child_is_list else ''
        name ='{child}{limit}'.format(child=self.parent_key_access, limit=child_limit_arg)
        child = GqlQuery().fields([child_edges], name).generate()
        node = GqlQuery().fields(['id', child, "createdAt"], name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges

    def get_query_input(self, created_at_min: str, created_at_max: str, after: typing.Optional[str]) -> dict:
        inputs = {"first": self.parent_per_page}
        if after:
            inputs["after"] = "\"{}\"".format(after)
        return inputs


Context.stream_objects['discount_codes_ql'] = DiscountCodes
Context.stream_objects['priceRules'] = Context.stream_objects['price_rules']
