import json
import shopify
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import SubStream, RESULTS_PER_PAGE

LOGGER = singer.get_logger()

class Metafields(SubStream):
    name = 'metafields'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    key_properties = ['id']


    def _sync_root(self):
        start_date = self.get_bookmark()
        count = 0

        def call_endpoint(page, start_date):
            return shopify.Metafield.find(
                # Max allowed value as of 2018-09-19 11:53:48
                limit=RESULTS_PER_PAGE,
                page=page,
                updated_at_min=start_date,
                updated_at_max=Context.tap_start,
                # Order is an undocumented query param that we believe
                # ensures the order of the results.
                order="updated_at asc")


        metafields = self.paginate_endpoint(call_endpoint, start_date)
        for metafield in metafields:
            yield metafield.to_dict()
            count += 1

        LOGGER.info('Shop Metafields Count = %s', count)

    def _sync_child(self, parent_obj, start_bookmark):
        count = 0

        def call_child_endpoint(page, start_date):
            return shopify.Metafield.find(
                # Max allowed value as of 2018-09-19 11:53:48
                limit=RESULTS_PER_PAGE,
                page=page,
                updated_at_min=start_bookmark,
                updated_at_max=Context.tap_start,
                # Order is an undocumented query param that we believe
                # ensures the order of the results.
                order="updated_at asc",
                **{"metafield[owner_id]": parent_obj.id,
                   "metafield[owner_resource]": self.parent_type})

        metafields = self.paginate_endpoint(call_child_endpoint, start_bookmark)
        for metafield in metafields:
            yield metafield.to_dict()
            count += 1

        LOGGER.info('%s Metafields Count = %s',
                    self.parent_type.replace('_', ' ').title(),
                    count)

    def sync(self, parent_obj=None, start_bookmark=None):
        if self.parent_type is None:
            records = self._sync_root()
        else:
            records = self._sync_child(parent_obj, start_bookmark)

        for rec in records:
            value_type = rec.get("value_type")
            if value_type and value_type == "json_string":
                value = rec.get("value")
                rec["value"] = json.loads(value) if value is not None else value
            yield (self.name, rec)

Context.stream_objects['metafields'] = Metafields
Context.streams['metafields'] = []
