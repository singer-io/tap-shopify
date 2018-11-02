# TL;DR We appear to be unable to capture updates to existing records
# without using updated_at_min and max with page. How should we
# implement that?

import shopify
import os

session = shopify.Session(os.environ['shopify_store'], os.environ['shopify_key'])
shopify.ShopifyResource.activate_session(session)

# The following sets up a stable since_id range query to the best of
# our knowledge.

since_id = 707018293321
since_id_pagination_end_id = 708235001929
since_id_min_id = None
since_id_max_id = None
since_id_min_updated_at = None
since_id_max_updated_at = None
since_id_id_set = set()

query_params = {
    "since_id": since_id,
    "limit": 250,
    "status": "any"
}

while True:
    orders = shopify.Order.find(**query_params)

    for order in [o for o in orders if o.id <= since_id_pagination_end_id]:
        if not since_id_min_id or since_id_min_id > order.id:
            since_id_min_id = order.id
        if not since_id_max_id or since_id_max_id < order.id:
            since_id_max_id = order.id
        if not since_id_min_updated_at or since_id_min_updated_at > order.updated_at:
            since_id_min_updated_at = order.updated_at
        if not since_id_max_updated_at or since_id_max_updated_at < order.updated_at:
            since_id_max_updated_at = order.updated_at
        since_id_id_set.add(order.id)

    if [order.id for order in orders if order.id == since_id_pagination_end_id]:
        print("since_id done!")
        break

    new_since_id = orders[-1].id
    print("since_id: setting since_id to {}".format(new_since_id))
    query_params['since_id'] = new_since_id

# The following sets up a paginated updated_at_min and max query to
# the best of our knowledge (the kind of query you're asking us to
# cease.

page_updated_at_min = since_id_min_updated_at
page_updated_at_max = since_id_max_updated_at
page_min_updated_at = None
page_max_updated_at = None
page_id_id_set = set()

query_params = {
    "page": 1,
    # Should result in the same resultset because the updated at
    # bookends are stable?
    "updated_at_min": page_updated_at_min,
    "updated_at_max": page_updated_at_max,
    "limit": 250,
    "status": "any"
}

while True:
    orders = shopify.Order.find(**query_params)

    for order in orders:
        if not page_min_updated_at or page_min_updated_at > order.updated_at:
            page_min_updated_at = order.updated_at
        if not page_max_updated_at or page_max_updated_at < order.updated_at:
            page_max_updated_at = order.updated_at
        page_id_id_set.add(order.id)

    if len(orders) < 250:
        print("page: done!")
        break

    query_params['page'] += 1
    print("page: setting page to {}".format(query_params['page']))

print("""In the above queries, we observed that using since_id ({}) across a
stable range of ids ({}-{}) produces a consistent minimum ({}) and
maximum ({}) updated_at value.

If you change to using an updated_at_min ({}) and updated_at_max ({})
range with `page` pagination, you observe ids being updated that exist
below the id range ({}-{}).

For our page + updated_at range query, {} IDs exist outside of the set
returned by the since_id query""".format(
        since_id,
        since_id_min_id,
        since_id_max_id,
        since_id_min_updated_at,
        since_id_max_updated_at,
        page_min_updated_at,
        page_max_updated_at,
        since_id_min_id,
        since_id_max_id,
        len(page_id_id_set - since_id_id_set)))

# $ pip list freeze
# Package           Version   Location
# ----------------- --------- ---------------------
# astroid           2.0.4
# attrs             16.3.0
# backcall          0.1.0
# backoff           1.3.2
# certifi           2018.8.24
# chardet           3.0.4
# decorator         4.3.0
# idna              2.7
# ipdb              0.11
# ipython           6.5.0
# ipython-genutils  0.2.0
# isort             4.3.4
# jedi              0.13.1
# jsonschema        2.6.0
# lazy-object-proxy 1.3.1
# mccabe            0.6.1
# parso             0.3.1
# pexpect           4.6.0
# pickleshare       0.7.5
# pip               18.1
# prompt-toolkit    1.0.15
# ptyprocess        0.6.0
# pyactiveresource  2.1.2
# Pygments          2.2.0
# pylint            2.1.1
# python-dateutil   2.7.3
# pytz              2018.4
# PyYAML            3.13
# requests          2.20.0
# setuptools        40.4.3
# ShopifyAPI        3.1.0
# simplegeneric     0.8.1
# simplejson        3.11.1
# singer-python     5.2.2
# singer-tools      0.4.1
# six               1.11.0
# strict-rfc3339    0.7
# tap-shopify       0.5.0     /opt/code/tap-shopify
# terminaltables    3.1.0
# traitlets         4.3.2
# typed-ast         1.1.0
# typing            3.6.6
# urllib3           1.23
# wcwidth           0.1.7
# wheel             0.32.1
# wrapt             1.10.11

# Within the following 3 runs you can see that the number of IDs that
# exist outside of the range of the since_id version which also seems
# very concerning.

# $ shopify_store=<redacted> shopify_key=<redacted>  python orders.py
# since_id: setting since_id to 707616505929
# since_id done!
# page: setting page to 2
# page: setting page to 3
# page: setting page to 4
# page: setting page to 5
# page: setting page to 6
# page: setting page to 7
# page: setting page to 8
# page: setting page to 9
# page: setting page to 10
# page: done!
# In the above queries, we observed that using since_id (707018293321) across a
# stable range of ids (707019243593-708235001929) produces a consistent minimum (2018-10-31T19:28:52-04:00) and
# maximum (2018-11-01T12:45:49-04:00) updated_at value.

# If you change to using an updated_at_min (2018-10-31T19:28:52-04:00) and updated_at_max (2018-11-01T12:45:49-04:00)
# range with `page` pagination, you observe ids being updated that exist
# below the range (2018-10-31T19:28:52-04:00-2018-11-01T12:45:49-04:00).

# For our page + updated_at range query, 1989 IDs exist outside of the set
# returned by the since_id query

# $ shopify_store=<redacted> shopify_key=<redacted>  python orders.py
# since_id: setting since_id to 707616505929
# since_id done!
# page: setting page to 2
# page: setting page to 3
# page: setting page to 4
# page: setting page to 5
# page: setting page to 6
# page: setting page to 7
# page: setting page to 8
# page: setting page to 9
# page: setting page to 10
# page: done!
# In the above queries, we observed that using since_id (707018293321) across a
# stable range of ids (707019243593-708235001929) produces a consistent minimum (2018-10-31T19:28:52-04:00) and
# maximum (2018-11-01T12:45:49-04:00) updated_at value.

# If you change to using an updated_at_min (707019243593) and updated_at_max (708235001929)
# range with `page` pagination, you observe ids being updated that exist
# below the id range (2018-10-31T19:28:52-04:00-2018-11-01T12:45:49-04:00).

# For our page + updated_at range query, 1986 IDs exist outside of the set
# returned by the since_id query

# $ shopify_store=<redacted> shopify_key=<redacted>  python orders.py
# since_id: setting since_id to 707616505929
# since_id done!
# page: setting page to 2
# page: setting page to 3
# page: setting page to 4
# page: setting page to 5
# page: setting page to 6
# page: setting page to 7
# page: setting page to 8
# page: setting page to 9
# page: setting page to 10
# page: done!
# In the above queries, we observed that using since_id (707018293321) across a
# stable range of ids (707019243593-708235001929) produces a consistent minimum (2018-10-31T19:28:52-04:00) and
# maximum (2018-11-01T12:45:49-04:00) updated_at value.

# If you change to using an updated_at_min (2018-10-31T19:28:52-04:00) and updated_at_max (2018-11-01T12:45:49-04:00)
# range with `page` pagination, you observe ids being updated that exist
# below the id range (707019243593-708235001929).

# For our page + updated_at range query, 1986 IDs exist outside of the set
# returned by the since_id query
