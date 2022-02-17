import shopify

import json
import datetime
import functools
import math
import sys

import singer
from singer import metrics, utils

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()

RESULTS_PER_PAGE = 175

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 14

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5

# class DraftOrders(Stream):
#     name = 'draft_orders'
#     replication_object = shopify.DraftOrder
#     status_value = "open,invoice_sent,completed"

# Context.stream_objects['draft_orders'] = DraftOrders

class DraftOrders(Stream):
    name = 'draft_orders'
    replication_object = shopify.DraftOrder
    status_value = "open,invoice_sent,completed"

    def addCreatedByWho(self, obj):
        query = '{ draftOrder(id: "' + obj['admin_graphql_api_id'] + '") { events(first: 1)  { edges { node { ... on BasicEvent { id message } } } } } }'
        res = json.loads(shopify.GraphQL().execute(query))
        msg = res['data']['draftOrder']['events']['edges'][0]['node']['message']
        name = msg.replace(" created this draft order.", "")
        obj['created_by'] = name
        return obj
    
    # def get_objects(self):
    #     updated_at_min = self.get_bookmark()

    #     stop_time = singer.utils.now().replace(microsecond=0)
    #     date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
    #     results_per_page = Context.get_results_per_page(RESULTS_PER_PAGE)

    #     # Page through till the end of the resultset
    #     while updated_at_min < stop_time:
    #         # Bookmarking can also occur on the since_id
    #         since_id = self.get_since_id() or 1

    #         if since_id != 1:
    #             LOGGER.info("Resuming sync from since_id %d", since_id)

    #         # It's important that `updated_at_min` has microseconds
    #         # truncated. Why has been lost to the mists of time but we
    #         # think it has something to do with how the API treats
    #         # microseconds on its date windows. Maybe it's possible to
    #         # drop data due to rounding errors or something like that?
    #         updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
    #         if updated_at_max > stop_time:
    #             updated_at_max = stop_time
    #         while True:
    #             status_key = self.status_key or "status"
    #             status_value = self.status_value or "any"
    #             query_params = {
    #                 "since_id": since_id,
    #                 "updated_at_min": updated_at_min,
    #                 "updated_at_max": updated_at_max,
    #                 "limit": results_per_page,
    #                 status_key: status_value
    #             }

    #             with metrics.http_request_timer(self.name):
    #                 objects = self.call_api(query_params)

                
    #             for obj in objects:
    #                 if obj.id < since_id:
    #                     # This verifies the api behavior expectation we
    #                     # have that all results actually honor the
    #                     # since_id parameter.
    #                     raise OutOfOrderIdsError("obj.id < since_id: {} < {}".format(
    #                         obj.id, since_id))
    #                 # add 'createdBy' to obj and yield obj in dict <----- main change in this function
    #                 yield self.addCreatedByWho(obj.to_dict())

    #             # You know you're at the end when the current page has
    #             # less than the request size limits you set.
    #             if len(objects) < results_per_page:
    #                 # Save the updated_at_max as our bookmark as we've synced all rows up in our
    #                 # window and can move forward. Also remove the since_id because we want to
    #                 # restart at 1.
    #                 Context.state.get('bookmarks', {}).get(self.name, {}).pop('since_id', None)
    #                 self.update_bookmark(utils.strftime(updated_at_max))
    #                 break

    #             if objects[-1]['id'] != max([o['id'] for o in objects]):
    #                 # This verifies the api behavior expectation we have
    #                 # that all pages are internally ordered by the
    #                 # `since_id`.
    #                 raise OutOfOrderIdsError("{} is not the max id in objects ({})".format(
    #                     objects[-1]['id'], max([o['id'] for o in objects])))
    #             since_id = objects[-1]['id']

    #             # Put since_id into the state.
    #             self.update_bookmark(since_id, bookmark_key='since_id')

    #         updated_at_min = updated_at_max        

            

    # overwrite sync function to add 'createdBy'
    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield self.addCreatedByWho(obj.to_dict())
   

    # def sync(self):
    #     """Yield's processed SDK object dicts to the caller.

    #     This is the default implementation. Get's all of self's objects
    #     and calls to_dict on them with no further processing.
    #     """
    #     for obj in self.get_objects():
    #         yield obj
        

Context.stream_objects['draft_orders'] = DraftOrders
