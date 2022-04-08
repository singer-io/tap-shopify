import shopify

from tap_shopify.streams.graph_ql_stream import GraphQlStream
from tap_shopify.context import Context
from gql_query_builder import GqlQuery


class Giftcards(GraphQlStream):
    name = 'giftCards'
    replication_key = 'createdAt'
    replication_object = shopify.GiftCard
    prefix = 'gid://shopify/GiftCard/'
    fragment_cols = {"balance": "MoneyV2", "initialValue": "MoneyV2"}
    fragment_entities = {"MoneyV2": ["amount", "currencyCode"]}
    selection_fields = {"customer": "gid://shopify/Customer/", "order": "gid://shopify/Order/"}

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

                # used to extract id from object inside main object
                if prop_name in list(self.selection_fields):
                    selected_field = GqlQuery().fields(["id"], name=prop_name).generate()
                    ql_fields.append(selected_field)
                    continue

                ql_fields.append(prop_name)
        return ql_fields

    def get_graph_edges(self) -> str:
        fields = self.get_graph_ql_prop(self.get_table_schema())
        node = GqlQuery().fields(fields, name='node').generate()
        edges = GqlQuery().fields(['cursor', node], name='edges').generate()
        return edges

    def transform_obj(self, obj):
        if obj.get("id", False):
            obj["id"] = obj["id"].replace(self.prefix, '')
        if obj.get("balance", False):
            obj["currency"] = obj["balance"].get("currencyCode", False)
            obj["balance"] = obj["balance"].get("amount", False)
        if obj.get("initialValue", False):
            obj["initialValue"] = obj["initialValue"].get("amount", False)
        for k, v in self.selection_fields.items():
            if obj.get(k, False):
                obj[k] = obj[k].get("id", False) and obj[k].get("id", False).replace(v, '')

        return obj


Context.stream_objects['giftcards'] = Giftcards
