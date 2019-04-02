#!/usr/bin/env python3
import math
import shopify
from datetime import datetime, timedelta
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
WRITE_TO_TARGET = False


def get_hourly_chunks(start, end, num_hours=1):
    ranges = []
    NUM_SECONDS = num_hours * 60 * 60
    st = utils.strptime_with_tz(start)
    ed = utils.strptime_with_tz(end)
    
    while st < ed:
        curr_ed = st + timedelta(seconds=NUM_SECONDS)
        if curr_ed > ed:
            curr_ed = ed
        ranges.append({'updated_at_min': st, 'updated_at_max': curr_ed})
        st = curr_ed + timedelta(seconds=1)

    return ranges

class Error(Exception):
    """Base exception for the API interaction module"""

class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""

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
        self.DT_FMT = '%Y-%m-%dT%H:%M:%S'


    async def _get_async(self, url, headers=None, params=None, retry_attempt=0):
        headers = {**headers, "Connection": "close"} if headers else {"Connection": "close"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            sleep_time_str = math.floor(float(response.headers.get('Retry-After')))
                            LOGGER.info("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
                            await asyncio.sleep(sleep_time_str)
                            return await self._get_async(url, headers, params, retry_attempt=retry_attempt+1)
                        elif response.status in range(500, 599):
                            if retry_attempt <= self.retry_limit:
                                return await self._get_async(url, headers, params, retry_attempt=retry_attempt+1)
                            else:
                                msg = "Failed after {} retry attempts".format(self.retry_limit)
                                LOGGER.error(msg)
                                raise Exception(msg)
        except Exception as e:
            if retry_attempt <= self.retry_limit:
                return await self._get_async(url, headers, params, retry_attempt=retry_attempt+1)
            else:
                msg = "Failed after {} retry attempts.\nError:\n{}\n".format(self.retry_limit, e)
                LOGGER.error(msg)
                raise Exception(msg)

    async def _request(self, job):
        endpoint = '{}.json'.format(self.endpoint)
        url = self.base_url + endpoint
        since_id = 1
        DT_FMT = '%Y-%m-%dT%H:%M:%S'
        results = []
        while True:
            params = {
                **self.params,
                "since_id": since_id,
                "updated_at_min": utils.strftime(job['updated_at_min'], format_str=DT_FMT),
                "updated_at_max": utils.strftime(job['updated_at_max'], format_str=DT_FMT)
            }
            resp = await self._get_async(url, params=params)
            LOGGER.info("GET {}{}?{}".format(self.shop_display_url, endpoint, urlencode(params)))

            objects = []
            if self.result_key in resp:
                for obj in resp[self.result_key]:
                    if obj['id'] < since_id:
                        err_msg = "obj['id'] < since_id: {} < {}".format(obj['id'], since_id)
                        raise OutOfOrderIdsError(err_msg)
                    objects.append(obj)
            results += objects

            if len(objects) < self.results_per_page:
                break

            max_id = max([o['id'] for o in objects])
            if objects[-1]['id'] != max_id:
                err_msg = "{} is not the max id in objects ({})".format(objects[-1]['id'], max_id)
                raise OutOfOrderIdsError(err_msg)

            since_id = objects[-1]['id']
        return results

    async def _runner(self):
        hour_windows = [h for h in get_hourly_chunks(self.params['updated_at_min'], self.params['updated_at_max'])]
        chunked_jobs = utils.chunk(hour_windows, self.bucket_size)

        for chunk_of_jobs in chunked_jobs:
            futures = [self._request(i) for i in chunk_of_jobs]
            for i, future in enumerate(asyncio.as_completed(futures)):
                results = await future
                self._write_singer_records(results)

    def _write_singer_records(self, recs):
        with Transformer() as transformer:
            for rec in recs:
                if WRITE_TO_TARGET:
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


def sync_async(config):
    """
        Gets objects for endpoint, and writes singer records.
        Returns the total number of records received and
        emitted to target.
    """
    updated_at_min = utils.strptime_with_tz(config['start_date'])
    end_date = config['end_date']
    updated_at_max = utils.strptime_with_tz(end_date) if end_date is not None else singer.utils.now().replace(microsecond=0)
    results_per_page = config['results_per_page']
    
    DT_FMT = '%Y-%m-%dT%H:%M:%S'
    query_params = {
        "updated_at_min": utils.strftime(updated_at_min, format_str=DT_FMT),
        "updated_at_max": utils.strftime(updated_at_max, format_str=DT_FMT),
        "status": "any"
    }

    return RunAsync.sync(
        schema = config['stream_schema'],
        stream_id = config['stream_id'],
        endpoint = config['stream_endpoint'],
        result_key = config['stream_result_key'],
        params = query_params,
        retry_limit = config['max_retries'],
        results_per_page = results_per_page
    )


@utils.handle_top_exception(LOGGER)
def SIDD_ASYNC_TAP(config):
    shop_url = "https://{k}:{p}@{s}.myshopify.com/admin".format(k=config['api_key'],p=config['api_password'],s=config['shop_name'])
    shopify.ShopifyResource.set_site(shop_url)

    start_time = time.time()
    if WRITE_TO_TARGET:
        singer.write_schema(config['stream_id'], config['stream_schema'], config['key_properties'], bookmark_properties=config['replication_key'])
    rec_count = sync_async(config)
    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))

    return (rec_count, duration)