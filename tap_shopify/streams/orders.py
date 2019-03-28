import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
import singer
from singer import utils
import asyncio
import aiohttp
import math
from urllib.parse import urlencode


LOGGER = singer.get_logger()

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
                    if 'page' in params:
                        resp['page'] = params['page']
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


class Orders(Stream):
    name = 'orders'
    replication_object = shopify.Order
    replication_object_async = OrderAsync
    endpoint = "/orders"
    result_key = "orders"

Context.stream_objects['orders'] = Orders
