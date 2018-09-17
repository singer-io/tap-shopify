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

start = datetime.now()


def request_orders(lim):
    try:
        return shopify.Order.find(limit=lim)    
    except Exception as e:
        resp = e.response
        sleep_time_str = resp.headers['Retry-After']
        print("!!!!!!!!!!!!!!!!!!")
        print("Received 429 -- sleeping for {} seconds".format(sleep_time_str))
        print("!!!!!!!!!!!!!!!!!!")
        sys.stdout.flush()
        time.sleep(math.floor(float(sleep_time_str)))
        return []

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
    # the_orders = request_orders(1)
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


