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

start = datetime.now()


def request_orders(lim):
    req_url = shop_url + '/orders.json'
    req_params = {"limit": str(lim)}
    resp = requests.get(req_url,params=req_params)
    if resp.status_code == 429:
        sleep_time_str = resp.headers['Retry-After']
        print("!!!!!!!!!!!!!!!!!!")
        print("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
        print("!!!!!!!!!!!!!!!!!!")
        sys.stdout.flush()
        time.sleep(math.floor(float(sleep_time_str)))
    return resp.json()['orders'] if 'orders' in resp.json() else []

iterations = 0
while True:
    iterations += 1
    #if iterations == 5:
    #    break
    print("---------------------------")
    print("Running for {} seconds".format(
        (datetime.now() - start).seconds))
    print("Making request %d" % iterations)

    req_start = datetime.now()
    the_orders = request_orders(250)
    # Using 1 because it hits the 429 faster
    #the_orders = request_orders_req(1)
    req_duration = datetime.now() - req_start
    req_duration_ms = req_duration.seconds*1000 + math.floor(req_duration.microseconds/1000)
    print("Retrieved {} orders in {} ms".format(
        len(the_orders),
        req_duration_ms))
    
    total_duration = (datetime.now() - start)
    total_duration_seconds = total_duration.seconds
    total_duration_milliseconds = total_duration_seconds * 1000 + math.floor(total_duration.microseconds/1000)
    print("Made {} requests in {} seconds (average: {} ms/request)".format (
        iterations, 
        total_duration_seconds,
        math.floor(total_duration_milliseconds/iterations)))
    sys.stdout.flush()


