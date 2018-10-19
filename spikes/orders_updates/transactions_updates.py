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

def shopify_request(endpoint,req_params={}):
    req_url = '{}/{}'.format(shop_url, endpoint)
    resp = requests.get(req_url, params=req_params)
    return resp.json()

def shopify_post(endpoint, content):
    post_url = '{}/{}'.format(shop_url, endpoint)
    resp = requests.post(post_url, json=content)
    return resp.json()


stream_params = {
    "limit": 250,
    "page": 1
}


# -------- 1 Create order with "authorization" transaction ---------
new_order = {
  "order": {
    "line_items": [
      {
        "title": "Large Brown Alligator Boots",
        "price": 74.99,
        "grams": "1300",
        "quantity": 3,
        "tax_lines": [
          {
            "price": 13.5,
            "rate": 0.06,
            "title": "State tax"
          }
        ]
      }
    ],
    "transactions": [
      {
        "kind": "authorization",
        "status": "success",
        "amount": 250.00
      }
    ],
    "total_tax": 13.5,
    "currency": "USD"
  }
}
order_post_endpoint = 'orders.json'
orders_post_resp = shopify_post(order_post_endpoint, new_order)
new_order_id = orders_post_resp['order']['id']
order_created_updated_at = orders_post_resp['order']['updated_at']
new_order_line_item_id = orders_post_resp['order']['line_items'][0]['id']

time.sleep(5)

# ---------- 2 Add "capture" transaction to new order ----------
new_transaction = {
    "transaction": {
        "kind": "capture",
        "status": "success",
        "amount": 0.10
    }
}

transactions_post_endpoint = 'orders/{}/transactions.json'.format(new_order_id)
transactions_post_resp = shopify_post(transactions_post_endpoint, new_transaction)

# ---------- 3 Get updated_at after adding transaction ----------
order_get_endpoint = 'orders/{}.json'.format(new_order_id)
orders_get_resp = shopify_request(order_get_endpoint, stream_params)
transaction_added_updated_at = orders_get_resp['order']['updated_at']

time.sleep(5)

# ---------- 4 Add new metafield ----------
new_metafield = {
    "metafield": {
        "namespace": "inventory",
        "key": "warehouse",
        "value": 25,
        "value_type": "integer"
    }
}
metafields_post_endpoint = 'orders/{}/metafields.json'.format(new_order_id)
metafields_post_resp =  shopify_post(metafields_post_endpoint, new_metafield)


# ---------- 5 Get updated_at after adding metafield ----------
orders_get_resp = shopify_request(order_get_endpoint, stream_params)
metafield_added_updated_at = orders_get_resp['order']['updated_at']

#---------- 6 Calculate refund ----------
suggested_refund = {
    "refund": {
        "shipping": {
            "full_refund": True
        },
        "refund_line_items": [
            {
                "line_item_id": new_order_line_item_id,
                "quantity": 1,
                "restock_type": "no_restock"
            }
        ]
    }
}
suggested_refund_post_endpoint = 'orders/{}/refunds/calculate.json'.format(new_order_id)
suggested_refund_post_resp = shopify_post(suggested_refund_post_endpoint, suggested_refund)

# -------- 7 Add Refund --------
new_refund = {
  "refund": {
    "notify": True,
    "note": "wrong size",
    "shipping": {
      "full_refund": True
    },
    "refund_line_items": [
      {
        "line_item_id": suggested_refund_post_resp['refund']['refund_line_items'][0]['line_item_id'],
        "quantity": 1,
        "restock_type": "no_restock",
        "location_id": suggested_refund_post_resp['refund']['refund_line_items'][0]['location_id'],
      }
    ],
    "transactions": [
      {
        "parent_id": suggested_refund_post_resp['refund']['transactions'][0]['parent_id'],
        "amount": suggested_refund_post_resp['refund']['transactions'][0]['amount'],
        "kind": "refund",
        "gateway": "bogus"
      }
    ]
  }
}
# NOTE: Could not get new refund post to work.  Suggested refund works, but we get an error
# when we try to post a new refund: {'errors': {'refund': 'Required parameter missing or invalid'}}

#refunds_post_endpoint = 'orders/{}/refunds.json'.format(new_order_id)
#refunds_post_resp =  shopify_post(refunds_post_endpoint, new_metafield)

# 6 Verify that all updated_at's are different
print('Original updated_at: {}'.format(order_created_updated_at))
print('Transaction added updated_at: {}'.format(transaction_added_updated_at))
print('Metafield added updated_at: {}'.format(metafield_added_updated_at))
