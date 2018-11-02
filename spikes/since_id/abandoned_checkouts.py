import shopify
import os

session = shopify.Session(os.environ['shopify_store'], os.environ['shopify_key'])
shopify.ShopifyResource.activate_session(session)

updated_at_min = '2018-01-01'
updated_at_max = '2018-01-20'
since_id = 1

query_params = {
    "since_id": since_id,
    "updated_at_min": updated_at_min,
    "updated_at_max": updated_at_max,
    "limit": 250,
    "status": "any"
}

sum = 0
while True:
    checkouts = shopify.Checkout.find(**query_params)
    sum += len(checkouts)

    if len(checkouts) < 250:
        print("done!")
        break

    new_since_id = checkouts[-1].id
    print("setting since_id to {}".format(new_since_id))
    query_params['since_id'] = new_since_id

print("the sum for since_id: {}".format(sum))

page = 1
sum = 0
query_params = {
    "page": page,
    "updated_at_min": updated_at_min,
    "updated_at_max": updated_at_max,
    "limit": 250,
    "status": "any"
}

while True:
    checkouts = shopify.Checkout.find(**query_params)
    sum += len(checkouts)

    if len(checkouts) < 250:
        print("done!")
        break

    page += 1
    print("setting page to {}".format(page))
    query_params['page'] = page

print("the sum for page: {}".format(sum))
