#!/usr/bin/env python3
import math
import shopify
from datetime import datetime
import time
import sys
import requests
import time
import asyncio
import aiohttp
import json
import singer
from singer import utils

LOGGER = singer.get_logger()

from urllib.parse import urlparse, urlencode

SHOP_NAME = ''
API_KEY = ''
PASSWORD = ''
shop_url = "https://%s:%s@%s.myshopify.com/admin" % (API_KEY, PASSWORD, SHOP_NAME)
shopify.ShopifyResource.set_site(shop_url)
RESULTS_PER_PAGE = 250
ORDERS_ENDPOINT='/orders'




def request_orders(params):
    req_url = shop_url + '/orders.json'
    resp = requests.get(req_url, params=params)
    if resp.status_code == 429:
        sleep_time_str = resp.headers['Retry-After']
        print("!!!!!!!!!!!!!!!!!!")
        print("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
        print("!!!!!!!!!!!!!!!!!!")
        time.sleep(math.floor(float(sleep_time_str)))
        resp = requests.get(params)
    return resp.json()['orders'] if 'orders' in resp.json() else []


def print_stats(ls, num_orders):
    _fmt_duration = lambda x: str(round(x,2)) + "s"
    num_times = len(ls)
    total_run_time = sum([v['duration'] for v in ls.values()])
    avg_run_time = total_run_time/num_times

    print("  Total Num. Runs : {}".format(num_times))
    print("  Num. Orders/Run : {}".format(num_orders))
    print("  Total Run Time  : {}".format(_fmt_duration(total_run_time)))
    print("  Avg. Run Time   : {}".format(_fmt_duration(avg_run_time)))

    div = "  " + "-"*46
    print("{d}\n   Granular Stats\n{d}".format(d=div))
    for i, v in ls.items():
        print("    Run {} (num_results={}) : {}".format(i+1, v['num_rows'],  _fmt_duration(v['duration'])))
    print("{d}\n".format(d=div))


class Error(Exception):
    """Base exception for the API interaction module"""

class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""

def since_id_bs():
        updated_at_min = utils.strptime_with_tz("2019-02-01 00:00:00")
        stop_time = utils.strptime_with_tz("2019-03-27 20:00:00")
        results_per_page = 250
        curr_since_id = 1
        final = []

        while updated_at_min < stop_time:
            since_id = curr_since_id

            if since_id != 1:
                LOGGER.info("Resuming sync from since_id %d", since_id)

            updated_at_max = stop_time
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            while True:
                query_params = {
                    "since_id": since_id,
                    "updated_at_min": updated_at_min,
                    "updated_at_max": updated_at_max,
                    "limit": results_per_page,
                    "status": "any"
                }
                objects = shopify.Order.find(**query_params)
                for obj in objects:
                    if obj.id < since_id:
                        raise OutOfOrderIdsError("obj.id < since_id: {} < {}".format(
                            obj.id, since_id))
                    final.append(obj.to_dict())

                if len(objects) < results_per_page:
                    return final

                if objects[-1].id != max([o.id for o in objects]):
                    # This verifies the api behavior expectation we have
                    # that all pages are internally ordered by the
                    # `since_id`.
                    raise OutOfOrderIdsError("{} is not the max id in objects ({})".format(
                        objects[-1].id, max([o.id for o in objects])))
                since_id = objects[-1].id

                # Put since_id into the state.
                curr_since_id = since_id

            updated_at_min = updated_at_max


class OrderAsync(shopify.Order):
    def __init__(self, endpoint, retry_limit, results_per_page, params):
        super()
        self.retry_limit = retry_limit
        self.results_per_page = results_per_page
        self.params = params
        self.params['limit'] = str(self.results_per_page)
        self.base_url = shopify.ShopifyResource.get_site()
        self.current_shop = shopify.Shop.current()
        self.bucket_size = 80 if self.current_shop.plan_name == "shopify_plus" else 40
        self.shop_display_url = "https://{}".format(self.current_shop.myshopify_domain)
        self.endpoint = endpoint

    async def _get_async(self, url, headers=None, params=None, retry_attempt=0):
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers, params=params) as response:
                if response.status == 429:
                    sleep_time_str = math.floor(float(response.headers.get('Retry-After')))
                    LOGGER.info("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
                    await asyncio.sleep(sleep_time_str)
                    return await self._get_async(url, headers, params, retry_attempt=retry_attempt+1)
                if response.status in range(500, 599):
                    return await self._get_async(url, headers, params, retry_attempt=retry_attempt+1)
                else:
                    resp = await response.json()
                    resp['page'] = params['page'] if 'page' in params else None
                    return resp

    async def _request_count(self):
        endpoint = '{}/count.json'.format(self.endpoint)
        url = self.base_url + endpoint
        resp = await self._get_async(url, params=self.params)
        LOGGER.info("GET {}{}?{}".format(self.shop_display_url, endpoint, urlencode(self.params)))
        return resp['count']

    async def _request(self, job):
        endpoint = '{}.json'.format(self.endpoint)
        url = self.base_url + endpoint
        resp = await self._get_async(url, params=job['params'])
        LOGGER.info("GET {}{}?{}".format(self.shop_display_url, endpoint, urlencode(job['params'])))
        return resp

    async def _runner(self):
        result_set_size = await self._request_count()
        num_pages = math.ceil(result_set_size/self.results_per_page)
        jobs = [{'params': { **self.params, 'page': p+1 }} for p in range(num_pages)]
        chunked_jobs = utils.chunk(jobs, self.bucket_size)

        all_jobs_results_dict = {}
        for chunk_of_jobs in chunked_jobs:
            futures = [self._request(i) for i in chunk_of_jobs]
            resp_dict = {}
            for i, future in enumerate(asyncio.as_completed(futures)):
                result = await future
                orders = result['orders'] if 'orders' in result else []
                resp_dict[result['page']] = orders
            all_jobs_results_dict = {**all_jobs_results_dict, **resp_dict}
        ordered_results = []
        for k,v in sorted(all_jobs_results_dict.items()):
            if len(v) > 0:
                ordered_results += v
        return ordered_results

    def Run(self):
        result = asyncio.run(self._runner())
        return result

    @classmethod
    def find(cls, endpoint, retry_limit=5, results_per_page=250, **kwargs):
        return OrderAsync(endpoint, retry_limit, results_per_page, params=kwargs).Run()


def main():
    # start_date = "2019-03-26T00:00:00-0:00"
    # end_date = "2019-03-27T20:00:00-0:00"
    start_date = "2019-03-26 00:00:00"
    end_date = "2019-03-27 20:00:00"
    updated_at_min = utils.strptime_with_tz(start_date)
    updated_at_max = utils.strptime_with_tz(end_date) if end_date is not None else singer.utils.now().replace(microsecond=0)
    DT_FMT = '%Y-%m-%dT%H:%M:%S'
    query_params = {
        "updated_at_min": utils.strftime(updated_at_min, format_str=DT_FMT),
        "updated_at_max": utils.strftime(updated_at_max, format_str=DT_FMT),
        "status": "any"
    }

    print(query_params)

    # 2014-04-25T16:15:47-04:00

    async_start_time = time.time()
    orders = OrderAsync.find(endpoint=ORDERS_ENDPOINT, **query_params)
    async_duration = time.time() - async_start_time
    print("Orders: ", len(orders))
    print("Duration: ", async_duration)

    # pg = [o['page'] for o in orders]
    # print(pg)
    # for v in orders:
    #     print("{}".format(len(v)))
    # for k,v in orders.items():
    #     print("Page {}: {}".format(k, len(v)))
    
    # since_id_orders = since_id_bs()

    # print("NUM. ORDERS RECV Request: {}".format(len(orders)))
    # print("NUM. ORDERS RECV Since_ID: {}".format(len(since_id_orders)))

    # orders_ids = [x['id'] for x in orders]
    # since_id_orders = [x['id'] for x in since_id_orders]


    # not_in_since_orders_ids = []
    # for o in orders_ids:
    #     if o not in since_id_orders:
    #         not_in_since_orders_ids.append(o)
    
    # not_in_orders_ids = []
    # for o in since_id_orders:
    #     if o not in orders_ids:
    #         not_in_orders_ids.append(o)

    # print('not_in_since_orders_ids: ', not_in_since_orders_ids)
    # print('not_in_orders_ids: ', not_in_orders_ids)


    # TOTAL ORDERS: 127407
# def main():
#     NUM_TIMES_TO_RUN = 1
#     BUCKET_SIZE = 80
#     NUM_ORDERS = 40000
#     sync_durations = {}
#     async_durations = {}
    
#     sleep_between_sync_and_async = 5
    
#     for n in range(NUM_TIMES_TO_RUN):
#         order_jobs = int(NUM_ORDERS/RESULTS_PER_PAGE)
#         params = {"limit": str(RESULTS_PER_PAGE)}
#         # Running Sync
#         sync_start_time = time.time()
#         sync_results = []
#         for j in range(order_jobs):
#             sync_results += request_orders(params)
#         sync_duration = time.time() - sync_start_time
#         sync_durations[n] = {'duration': sync_duration, 'num_rows': len(sync_results)}
        
#         print("Sleeping {}s between sync and async just to clear out".format(sleep_between_sync_and_async))
#         time.sleep(sleep_between_sync_and_async)

#         Running Async
#         async_start_time = time.time()
#         async_jobs = [params for p in range(order_jobs)]
#         async_jobs_chunked = chunks(async_jobs, BUCKET_SIZE)
#         async_results = []
#         for chunk in async_jobs_chunked:
#             async_results += GetOrdersAsync(shop_url, chunk).Run()
#         async_duration = time.time() - async_start_time
#         async_durations[n] = {'duration': async_duration, 'num_rows': len(async_results)}

#     div = "="*50

#     print("{d}\n Sync Stats\n{d}".format(d=div))
#     print_stats(sync_durations, NUM_ORDERS)
#     print("{d}\n".format(d=div))

#     print("{d}\n Async Stats\n{d}".format(d=div))
#     print_stats(async_durations, NUM_ORDERS)
#     print("{d}\n".format(d=div))


if __name__ == '__main__':
    main()