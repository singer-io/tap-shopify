#!/usr/bin/env python3
import math
import shopify
from datetime import datetime
import time
import sys
import asyncio
import aiohttp
import json
from urllib.parse import urlparse, urlencode
import singer
from singer import utils
from singer import Transformer


LOGGER = singer.get_logger()


SHOP_NAME = ''
API_KEY = ''
PASSWORD = ''
shop_url = "https://%s:%s@%s.myshopify.com/admin" % (API_KEY, PASSWORD, SHOP_NAME)
shopify.ShopifyResource.set_site(shop_url)


RESULT_KEY = 'orders'
ENDPOINT = '/orders'
MAX_RETRIES = 5
RESULTS_PER_PAGE = 250
START_DATE = "2019-03-26 00:00:00"
END_DATE = "2019-03-27 20:00:00"

with open('orders-schema.json') as f:
    STREAM = json.load(f)

STREAM_ID = STREAM['tap_stream_id']
SCHEMA = STREAM['schema']
KEY_PROPS = STREAM['key_properties']
REPLICATION_KEY = STREAM['replication_key']

# Streams that can run Async
ASYNC_AVAILABLE_STREAMS = ['orders', 'products', 'customers', 'abandoned_checkouts']

class RunAsync():
    def __init__(self, schema, stream_id, endpoint, result_key, params, retry_limit, results_per_page):
        self.schema = schema
        self.stream_id = stream_id
        self.endpoint = endpoint
        self.result_key = result_key
        self.params = params
        self.retry_limit = retry_limit
        self.results_per_page = results_per_page
        
        self.params['limit'] = str(self.results_per_page)
        self.base_url = shopify.ShopifyResource.get_site()
        self.current_shop = shopify.Shop.current()
        self.bucket_size = 80 if self.current_shop.plan_name == "shopify_plus" else 40
        self.shop_display_url = "https://{}".format(self.current_shop.myshopify_domain)
        self.rec_count = 0

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
                    return await response.json()

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

        for chunk_of_jobs in chunked_jobs:
            futures = [self._request(i) for i in chunk_of_jobs]
            for i, future in enumerate(asyncio.as_completed(futures)):
                result = await future
                if self.result_key in result:
                    self._write_singer_records(result[self.result_key])

    def _write_singer_records(self, recs):
        with Transformer() as transformer:
            for rec in recs:
                extraction_time = singer.utils.now()
                transformed_rec = transformer.transform(rec, self.schema)
                singer.write_record(self.stream_id, transformed_rec, time_extracted=extraction_time)
                self.rec_count += 1

    def Run(self):
        asyncio.run(self._runner())
        return self.rec_count

    @classmethod
    def sync(cls, schema, stream_id, endpoint, result_key, params, retry_limit=5, results_per_page=250):
        return RunAsync(schema, stream_id, endpoint, result_key, params, retry_limit, results_per_page).Run()


def sync_async():
    """
        Gets objects for endpoint, and writes singer records.
        Returns the total number of records received and
        emitted to target.
    """
    updated_at_min = utils.strptime_with_tz(START_DATE)
    end_date = END_DATE
    updated_at_max = utils.strptime_with_tz(end_date) if end_date is not None else singer.utils.now().replace(microsecond=0)
    results_per_page = RESULTS_PER_PAGE
    
    DT_FMT = '%Y-%m-%dT%H:%M:%S'
    query_params = {
        "updated_at_min": utils.strftime(updated_at_min, format_str=DT_FMT),
        "updated_at_max": utils.strftime(updated_at_max, format_str=DT_FMT),
        "status": "any"
    }

    return RunAsync.sync(
        schema = SCHEMA,
        stream_id = STREAM_ID,
        endpoint = ENDPOINT,
        result_key = RESULT_KEY,
        params = query_params,
        retry_limit = MAX_RETRIES,
        results_per_page = results_per_page
    )


@utils.handle_top_exception(LOGGER)
def main():
    start_time = time.time()
    singer.write_schema(STREAM_ID, SCHEMA, KEY_PROPS, bookmark_properties=REPLICATION_KEY)
    rec_count = sync_async()
    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))

    LOGGER.info('--------------------------------------------')
    LOGGER.info("{}: {}".format(STREAM_ID, rec_count))
    LOGGER.info("Duration: {}".format(duration))
    LOGGER.info('--------------------------------------------')


if __name__ == '__main__':
    main()