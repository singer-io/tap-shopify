import json
import shopify
import singer
from shopify import Product

from tap_shopify.context import Context
from tap_shopify.streams.base import (RESULTS_PER_PAGE, OutOfOrderIdsError)
from tap_shopify.streams.graph_ql_stream import (GraphQlChildStream, shopify_error_handling)

LOGGER = singer.get_logger()


def get_selected_parents():
    for parent_stream in ['orders', 'customers', 'products', 'custom_collections']:
        yield Context.stream_objects[parent_stream]()


@shopify_error_handling
def get_metafields(parent_object, since_id):
    # This call results in an HTTP request - the parent object never has a
    # cache of this data so we have to issue that request.
    return parent_object.metafields(
        limit=RESULTS_PER_PAGE,
        since_id=since_id)


class Metafields(GraphQlChildStream):
    name = 'metafields_ql'
    replication_object = shopify.Metafield

    def get_objects(self):
        # Get parent objects, bookmarking at `metafield_<object_name>`
        for selected_parent in get_selected_parents():
            selected_parent.name = "metafield_{}".format(selected_parent.name)
            for parent_object in selected_parent.get_objects():
                yield from self.get_metadatafields(parent_object)

    def get_metadatafields(self, parent_object):
        since_id = 1
        while True:
            metafields = get_metafields(parent_object, since_id)
            for metafield in metafields:
                if metafield.id < since_id:
                    raise OutOfOrderIdsError("metafield.id < since_id: {} < {}".format(
                        metafield.id, since_id))
                yield metafield

            if isinstance(parent_object, Product) and "variants" in parent_object.attributes:
                for variant in parent_object.attributes["variants"]:
                    yield from self.get_metadatafields(variant)

            if len(metafields) < RESULTS_PER_PAGE:
                break

            if metafields[-1].id != max([o.id for o in metafields]):
                raise OutOfOrderIdsError("{} is not the max id in metafields ({})".format(
                    metafields[-1].id, max([o.id for o in metafields])))

            since_id = metafields[-1].id

    def sync(self):
        # Shop metafields
        for metafield in self.get_objects():
            metafield = metafield.to_dict()
            value_type = metafield.get("value_type")
            if value_type and value_type == "json_string":
                value = metafield.get("value")
                try:
                    metafield["value"] = json.loads(value) if value is not None else value
                except json.decoder.JSONDecodeError:
                    LOGGER.info("Failed to decode JSON value for metafield %s", metafield.get('id'))
                    metafield["value"] = value

            yield metafield


Context.stream_objects['metafields_ql'] = Metafields
