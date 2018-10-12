import json
import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling)

LOGGER = singer.get_logger()

class Metafields(Stream):
    # FIXME remove unnecessary overrides
    name = 'metafields'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    replication_object = shopify.Metafield
    key_properties = ['id']

    def get_selected_parents(self):
        # FIXME note all other parents
        for parent_stream in ['orders']:
            if Context.is_selected(parent_stream):
                yield Context.stream_objects[parent_stream]()

    def get_call_api_fn(self, obj):
        @shopify_error_handling()
        def call_api(page):
            # We always retrieve these wholesale since there's no obvious
            # way to bookmark them (the bookmark would only be valid
            # within the object)
            return obj.metafields(
                limit=RESULTS_PER_PAGE,
                page=page,
                order="updated_at asc")
        return call_api

    def get_objects(self):
        # Get shop metafields, should paginate fine
        yield from super().get_objects()
        # Get parent objects, bookmarking at `metafield_<object_name>`
        for selected_parent in self.get_selected_parents():
            # The name member controls many things, but most importantly
            # the bookmark key. This switches us over to the
            # `metafield_<parent_type>` bookmark. We track that separately
            # to make resetting individual streams easier.
            selected_parent.name = "metafield_{}".format(selected_parent.name)
            for parent_object in selected_parent.get_objects():
                selected_parent.call_api = self.get_call_api_fn(parent_object)
                yield from selected_parent.get_objects()

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
