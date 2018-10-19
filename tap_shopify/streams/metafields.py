import json
import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)


def get_selected_parents():
    for parent_stream in ['orders', 'customers']:
        if Context.is_selected(parent_stream):
            yield Context.stream_objects[parent_stream]()

@shopify_error_handling()
def get_metafields(parent_object, page):
    return parent_object.metafields(
        limit=RESULTS_PER_PAGE,
        page=page,
        order="updated_at asc")

class Metafields(Stream):
    name = 'metafields'
    replication_object = shopify.Metafield

    def get_objects(self, status="open"):
        # Get top-level shop metafields
        yield from super().get_objects()
        # Get parent objects, bookmarking at `metafield_<object_name>`
        for selected_parent in get_selected_parents():
            # The name member controls many things, but most importantly
            # the bookmark key. This switches us over to the
            # `metafield_<parent_type>` bookmark. We track that separately
            # to make resetting individual streams easier.
            selected_parent.name = "metafield_{}".format(selected_parent.name)
            for parent_object in selected_parent.get_objects():
                page = 1
                while True:
                    metafields = get_metafields(parent_object, page)
                    for metafield in metafields:
                        yield metafield
                    if len(metafields) < RESULTS_PER_PAGE:
                        break
                    page += 1

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
