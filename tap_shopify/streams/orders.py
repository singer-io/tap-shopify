import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order

    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            record = obj.to_dict()
            record["customer_id"] = record.get("customer") and record["customer"].get("id") or False
            yield record


Context.stream_objects['orders'] = Orders
