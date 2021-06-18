#!/usr/bin/env python

import math
import shopify
from datetime import datetime
import sys
import requests
import time

SHOP_NAME = 'stitchdatawearhouse'
API_KEY = sys.argv[1]
PASSWORD = sys.argv[2]

shop_url = "https://%s:%s@%s.myshopify.com/admin" % (API_KEY, PASSWORD, SHOP_NAME)
shopify.ShopifyResource.set_site(shop_url)


shopify_objects = [
    'orders',
    'checkouts',
    'metafields',
    'customers',
    'products',
    'collects',
    'custom_collections_products',
    'smart_collections_products',
    'smart_collections',
    'custom_collections'
]

def shopify_request(endpoint,req_params={}):
    req_url = '{}/{}'.format(shop_url, endpoint)
    resp = requests.get(req_url, params=req_params)
    return resp.json()

# Hit count endpoint using requests for each object
object_counts = {}
for shopify_object in shopify_objects:
        count_resp = shopify_request('{}/count.json'.format(shopify_object))
        if 'count' in count_resp:
            object_counts[shopify_object] = count_resp['count']
            print("{} {}".format(object_counts[shopify_object], shopify_object))


sub_streams = {
    "orders": ["transactions", "metafields"],
    "customers": ["metafields"],
    "products": ["variants", "metafields"],
}

for stream, sub_streams in sub_streams.items():

    stream_count = 0
    sub_stream_counts = {}
    stream_params = {
        "limit": 250,
        "page": 1
    }
    sub_stream_params = {
        "limit": 250,
        "page": 1
    }
    current_page = 1

    while True:
        stream_resp = shopify_request('{}.json'.format(stream), stream_params)
        if (not stream in stream_resp) or (not any(stream_resp.get(stream))):
            break

        for stream_item in stream_resp[stream]:
            if 'id' in stream_item:
                time.sleep(1)
                for sub_stream in sub_streams:
                    if not sub_stream_counts.get(sub_stream):
                        sub_stream_counts[sub_stream] = 0

                    endpoint = '{}/{}/{}.json'.format(stream, stream_item['id'], sub_stream)
                    sub_stream_resp = shopify_request(endpoint, sub_stream_params)
                    if sub_stream in sub_stream_resp:
                        sub_stream_counts[sub_stream]  += len(sub_stream_resp[sub_stream])

                stream_count  += 1
        current_page += 1
        stream_params["page"] = current_page
    for sub_stream in sub_streams:
        print("{} {} across {} {}".format(sub_stream_counts[sub_stream], sub_stream, stream_count, stream))
