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
from singer import utils, metadata
from singer import Transformer
import requests

LOGGER = singer.get_logger()

DATE_WINDOW_SIZE=1
WRITE_TO_TARGET = False


class Error(Exception):
    """Base exception for the API interaction module"""

class OutOfOrderIdsError(Error):
    """Raised if our expectation of ordering by ID is violated"""

def get(url, headers=None, params=None, retry_attempt=0):
    with requests.get(url=url, headers=headers, params=params) as response:
        if response.status_code == 429:
            sleep_time_str = math.floor(float(response.headers.get('Retry-After')))
            LOGGER.info("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
            time.sleep(sleep_time_str)
            return get(url, headers, params, retry_attempt=retry_attempt+1)
        if response.status_code in range(500, 599):
            return get(url, headers, params, retry_attempt=retry_attempt+1)
        else:
            return response.json()

def request(endpoint, params, config):
    endpoint = '{}.json'.format(endpoint)
    base_url = shopify.ShopifyResource.get_site()
    current_shop = shopify.Shop.current()
    shop_display_url = "https://{}".format(current_shop.myshopify_domain)

    url = base_url + endpoint
    resp = get(url, params=params)
    LOGGER.info("GET {}{}?{}".format(shop_display_url, endpoint, urlencode(params)))
    return resp[config['stream_result_key']] if config['stream_result_key'] in resp else []


def get_objects(config):
    updated_at_min = utils.strptime_with_tz(config['start_date'])
    end_date = config['end_date']
    stop_time = utils.strptime_with_tz(end_date) if end_date is not None else singer.utils.now().replace(microsecond=0)
    date_window_size = DATE_WINDOW_SIZE
    results_per_page = config['results_per_page']

    while updated_at_min < stop_time:
        since_id = 1

        if since_id != 1:
            LOGGER.info("Resuming sync from since_id %d", since_id)

        updated_at_max = updated_at_min + timedelta(days=date_window_size)
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

            objects = request(config['stream_endpoint'], query_params, config)

            for obj in objects:
                if obj['id'] < since_id:
                    raise OutOfOrderIdsError("obj['id'] < since_id: {} < {}".format(
                        obj['id'], since_id))
                yield obj

            if len(objects) < results_per_page:
                break

            if objects[-1]['id'] != max([o['id'] for o in objects]):
                raise OutOfOrderIdsError("{} is not the max id in objects ({})".format(
                    objects[-1]['id'], max([o['id'] for o in objects])))
            since_id = objects[-1]['id']

        updated_at_min = updated_at_max


def sync(config):
    for obj in get_objects(config):
        yield obj


@utils.handle_top_exception(LOGGER)
def SIDD_ENDPOINT_TAP(config):
    shop_url = "https://{k}:{p}@{s}.myshopify.com/admin".format(k=config['api_key'],p=config['api_password'],s=config['shop_name'])
    shopify.ShopifyResource.set_site(shop_url)

    start_time = time.time()
    if WRITE_TO_TARGET:
        singer.write_schema(config['stream_id'], config['stream_schema'], config['key_properties'], bookmark_properties=config['replication_key'])
    rec_count = 0
    with Transformer() as transformer:
        for rec in sync(config):
            extraction_time = singer.utils.now()
            record_metadata = metadata.to_map(config['stream_metadata'])
            rec = transformer.transform(rec, config['stream_schema'], record_metadata)
            if WRITE_TO_TARGET:
                singer.write_record(config['stream_id'], rec, time_extracted=extraction_time)
            rec_count += 1
    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))

    return (rec_count, duration)