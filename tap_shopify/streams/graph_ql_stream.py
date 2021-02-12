import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)


class GraphQlChildStream(Stream):
    name = None
    replication_key = 'createdAt'
    replication_object = shopify.Transaction
    parent_key_access = None
    parent_name = None
    parent_replication_key = 'createdAt'
    parent_id_ql_prefix = ''

    def get_objects(self):
        selected_parent = Context.stream_objects[self.parent_name]()
        selected_parent.replication_key = self.parent_replication_key

        schema = self.get_table_schema()
        ql_properties = self.get_graph_ql_prop(schema)
        for parent_obj in selected_parent.get_children_by_graph_ql(self.parent_key_access, ql_properties):
            for child_obj in parent_obj[self.parent_key_access]:
                child_obj["parentId"] = self.transform_parent_id(parent_obj["id"])
                yield child_obj

    def sync(self):
        for child_obj in self.get_objects():
            yield child_obj

    def transform_parent_id(self, parent_id):
        parent_id = parent_id.replace(self.parent_id_ql_prefix, '')
        return parent_id
