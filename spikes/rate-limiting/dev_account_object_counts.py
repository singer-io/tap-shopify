#!/usr/bin/env python

import math
import shopify
from datetime import datetime
import sys
import requests    
import time

SHOP_NAME = 'stitchdatawearhouse'
API_KEY = 'd469b3cbbcd3f458b08235fd518c1b9b'
PASSWORD = 'd5f1e595a443430d6f26bc66433484d8'

shop_url = "https://%s:%s@%s.myshopify.com/admin" % (API_KEY, PASSWORD, SHOP_NAME)
shopify.ShopifyResource.set_site(shop_url)


shopify_objects = [
    'orders',
    'checkouts',
    'metafields',
    'customers',
    'products',
    'collects',
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

# Cycle through orders and count transactions
params = {"limit": 250}

orders_resp = shopify_request('orders.json', params)
transactions_count = 0
orders_count = 0
if 'orders' in orders_resp:
    for order in orders_resp['orders']:
        if 'id' in order:
            endpoint = 'orders/{}/transactions.json'.format(order['id'])
            transactions_resp = shopify_request(endpoint, params)
            if 'transactions' in transactions_resp:
                transactions_count += len(transactions_resp['transactions'])
        orders_count += 1
print("{} transactions across {} orders".format(transactions_count, orders_count))


        







    
