import json
import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import SubStream, RESULTS_PER_PAGE

LOGGER = singer.get_logger()

class Metafields(Stream):
    name = 'metafields'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    replication_object = shopify.Metafields
    key_properties = ['id']

    def get_selected_parents(self):
        Context something something
        pass

    def get_objects(self):
        # Get shop metafields, should paginate fine
        yield from super().get_objects()
        # Get parent objects, bookmarking at `metafield_<object_name>`
        for selected_parent in self.get_selected_parents():
            selected_parent.name = "metafield_{}".format(selected_parent.something)
            for parent_object in selected_parent.get_objects():
                # Maybe need pagination here?
                yield from parent_object.metafields()

    def sync(self):
        # Shop metafields
        for metafield in self.get_objects():
            metafield = metafield.to_dict()
            value_type = metafield.get("value_type")
            if value_type and value_type == "json_string":
                value = metafield.get("value")
                metafield["value"] = json.loads(value) if value is not None else value
            yield metafield

Context.stream_objects['metafields'] = Metafields
