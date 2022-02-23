import shopify

import json

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class DraftOrders(Stream):
    name = 'draft_orders'
    replication_object = shopify.DraftOrder
    status_value = "open,invoice_sent,completed"

    def add_created_by_who(self, obj):
        query = '{ draftOrder(id: "' + obj['admin_graphql_api_id'] + '") { events(first: 1)  { edges { node { ... on BasicEvent { id message } } } } } }'
        res = json.loads(shopify.GraphQL().execute(query))
        msg = res['data']['draftOrder']['events']['edges'][0]['node']['message']
        if "created this draft order" in msg:
            name = msg.replace(" created this draft order.", "")
            if name != "" :
                if "(deleted)" in name:
                    obj['created_by'] = name.replace(" (deleted)", "")
                else:
                    obj['created_by'] = name
            else:
                obj['created_by'] = None
        else:
            obj['created_by'] = None
        return obj

    # overwrite sync function to add 'createdBy'
    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield self.add_created_by_who(obj.to_dict())
   
      

Context.stream_objects['draft_orders'] = DraftOrders
