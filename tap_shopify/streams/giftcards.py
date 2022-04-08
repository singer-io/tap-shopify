import shopify

from tap_shopify.streams.graph_ql_stream import GraphQlStream
from tap_shopify.context import Context
from gql_query_builder import GqlQuery
import typing


class Giftcards(GraphQlStream):
    name = 'giftCards'
    replication_key = 'createdAt'
    replication_object = shopify.GiftCard
    fragment_cols = {"balance": "MoneyV2", "initialValue": "MoneyV2"}
    fragment_entities = {"MoneyV2": ["amount", "currencyCode"]}

    def get_table_schema(self) -> dict:
        streams = Context.catalog["streams"]
        schema = None
        for stream in streams:
            if stream["tap_stream_id"] == self.name.lower():
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
                if prop_name in list(self.fragment_cols):
                    fragment_entity = self.fragment_cols[prop_name]
                    fields = self.get_fragment_fields(prop_name, self.fragment_entities[fragment_entity])
                    ql_field = GqlQuery().fields(fields, name=prop_name).generate()
                    ql_fields.append(ql_field)
                    continue

                ql_fields.append(prop_name)
        return ql_fields

    def get_graph_edges(self) -> str:
        fields = self.get_graph_ql_prop(self.get_table_schema())
        node = GqlQuery().fields(fields, name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges


Context.stream_objects['giftcards'] = Giftcards
