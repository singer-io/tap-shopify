# Changelog

### 3.7.1
  * Query and extract multiple pages of products for each collection [#227](https://github.com/singer-io/tap-shopify/pull/231)

### 3.7.0
  * Refactor Orders Stream: standard GraphQL → Bulk API Migration [#227](https://github.com/singer-io/tap-shopify/pull/227)

### 3.6.2
  * Set max_size of pagination to 30 for fullfilment_orders [#230](https://github.com/singer-io/tap-shopify/pull/230)

## 3.6.1
  * Dependency upgrades [#228](https://github.com/singer-io/tap-shopify/pull/228)

## 3.6.0
  * Introduce new stream fulfillment_orders. [#222](https://github.com/singer-io/tap-shopify/pull/222)

## 3.5.0
  * Add fields in orders stream - retailLocation and location ID (within fulfillments) [#219](https://github.com/singer-io/tap-shopify/pull/219)
  * Skip the author field on missing read_users scope [#223](https://github.com/singer-io/tap-shopify/pull/223)

## 3.4.0
  * Introduce new stream order_shipping_lines. [#213](https://github.com/singer-io/tap-shopify/pull/213)
  * shippingLine is a nested object within the orders stream and it represents individual shipping methods.

## 3.3.2
  * Fix date_window bug[#221](https://github.com/singer-io/tap-shopify/pull/221)

## 3.3.1
  * Fix transformation bug for abandoned checkouts [#218](https://github.com/singer-io/tap-shopify/pull/218)

## 3.3.0
  * Dynamically generate graphql query [#214](https://github.com/singer-io/tap-shopify/pull/214)

## 3.2.1
  * Refactor refund and transactions sync logic to rely on updated parent objects [#212](https://github.com/singer-io/tap-shopify/pull/212)

## 3.2.0
  * Enhance tap with additional fields across streams [#209](https://github.com/singer-io/tap-shopify/pull/209)
  * Bookmarks are now updated after fetching records within the specified date range.

## 3.1.0
  * Add missing fields into the schema of orders and order_refunds stream [#208](https://github.com/singer-io/tap-shopify/pull/208)

## 3.0.0
  * Migrate Remaining Shopify Streams from REST API to GraphQL API [#201](https://github.com/singer-io/tap-shopify/pull/201)
  * Introduce new streams - `collections`, `metafields_collections`, `metafields_customers`, `metafields_orders`, `metafields_products`
  * Delete streams - `collects`, `custom_collections` and `metafields`

## 2.1.0
  * Optimize Shopify Metafields Sync Performance [#200](https://github.com/singer-io/tap-shopify/pull/200)
  * Include the new field (= inventoryItem) in the product variants schema.
  * Add retry for the interruptible sync error from the server side.

## 2.0.2
  * Update bookmark logic for transactions and order_refunds stream [#197](https://github.com/singer-io/tap-shopify/pull/197)

## 2.0.1
  * Fixed error handling for GraphQL client [#195](https://github.com/singer-io/tap-shopify/pull/195)

## 2.0.0
  * Deprecated REST Admin API for products
  * GraphQL support added for deprecated streams
  * New stream `Product Variants` added
  * More details here: [#193](https://github.com/singer-io/tap-shopify/pull/193)

## 1.10.0
  * Updates the Shopify SDK to 12.3.0
  * Updates API version used to 2024-01
  * Incarporates schema changes [#187](https://github.com/singer-io/tap-shopify/pull/187)

## 1.9.0
  * Updates to run on python 3.11 [#186](https://github.com/singer-io/tap-shopify/pull/186)

## 1.8.0
  * Updates the Shopify SDK to 12.3.0
  * Updates API version used to 2023_04
  * Adds and removes fields per Shopify API changelog for versions 2022_10, 2023_01, 2023_04 [#178](https://github.com/singer-io/tap-shopify/pull/178)

## 1.7.6
  * Add backoff for 404 error code [#159](https://github.com/singer-io/tap-shopify/pull/159)

## 1.7.5
  * Add backoff for ConnectionResetError [#169](https://github.com/singer-io/tap-shopify/pull/169)
## 1.7.4
  * Add backoff for IncompleteRead [#144](https://github.com/singer-io/tap-shopify/pull/144)

## 1.7.3
  * Update interrupted sync bookmark strategy [#166](https://github.com/singer-io/tap-shopify/pull/166)

## 1.7.2
  * Add URLError (connection reset by peer) to retry logic [#165](https://github.com/singer-io/tap-shopify/pull/165)

## 1.7.1
  * Update bookmarking logic [#143](https://github.com/singer-io/tap-shopify/pull/143)

## 1.7.0
  From [#157](https://github.com/singer-io/tap-shopify/pull/157):
  * API/SDK Upgrade to v12.0.1
  * New Field Additions to Schema
  * Fields removal from the schema

## 1.6.2
  * Add canonicalization of transaction receipts to OrderRefunds [#156] (https://github.com/singer-io/tap-shopify/pull/156)

## 1.6.1
  * Fixing Tranformation Issues [#149] (https://github.com/singer-io/tap-shopify/pull/149)

## 1.6.0
  * API/SDK Upgrade to v10.0.0 [#135] (https://github.com/singer-io/tap-shopify/pull/135)
  * New Field Additions to Schema [#140] (https://github.com/singer-io/tap-shopify/pull/140)

## 1.5.1
  * Request Timeout Implementation [#129](https://github.com/singer-io/tap-shopify/pull/129)

## 1.5.0
  * Adds `events` stream [#127](https://github.com/singer-io/tap-shopify/pull/127)

## 1.4.0
  * Add shop info in record [#115](https://github.com/singer-io/tap-shopify/pull/115)
  * Add inventory item data [#118] (https://github.com/singer-io/tap-shopify/pull/118)
  * Add Locations stream and TDL-13614: Add Inventory Levels stream [#114] (https://github.com/singer-io/tap-shopify/pull/114)
  * Added best practices [#116] (https://github.com/singer-io/tap-shopify/pull/116)
  * Discover mode should check token [#120] (https://github.com/singer-io/tap-shopify/pull/120)
## 1.3.6
  * Fixes a bug in the canonicalize function for a 'receipt' key existing with a null value [#119](https://github.com/singer-io/tap-shopify/pull/119)

## 1.3.5
  * Add `status` field to `Products` stream [#108](https://github.com/singer-io/tap-shopify/pull/108)

## 1.3.4
  * Correct abandoned_checkouts schema to correctly reflect some properties as arrays [#44](https://github.com/singer-io/tap-shopify/pull/44)

## 1.3.3.
  * Add `build` to the list of fields we canonicalize for the Transactions stream [#103](https://github.com/singer-io/tap-shopify/pull/103)

## 1.3.2
  * Add `python_requires` to `setup.py` [#101](https://github.com/singer-io/tap-shopify/pull/101)
    * We've tested the tap on `python 3.5.2` and `python 3.8.0`

## 1.3.1
  * Canonicalize `Timestamp` to `timestamp` on `Transactions.receipt` [#98](https://github.com/singer-io/tap-shopify/pull/98)

## 1.3.0
  This version ships both [#96][PR#96] and [#97][PR#97].

  From [#97][PR#97]:
  * removes the "untestable streams" list from all tests.
  * makes the test match the tap and expect the default page size to be 175, not 250
  * adds bookmarking to order_refunds
  * adds bookmarking to transactions
  * adds shopify error handling to transactions
    * The tests would fail with unhandled 429s
  * adds pagination to transactions

  From [#96][PR#96]:
  * Update the API version from `2020-10` to `2021-04`

  [PR#97]: https://github.com/singer-io/tap-shopify/pull/97/
  [PR#96]: https://github.com/singer-io/tap-shopify/pull/96

## 1.2.10
  * Add `null, object` to customer schema definition [#94](https://github.com/singer-io/tap-shopify/pull/94)

## 1.2.9
  * Bumps `singer-python` from `5.11.0` to `5.12.1` [#91](https://github.com/singer-io/tap-shopify/pull/91)

## 1.2.8
  * Modified schema so that all fields using `multipleOf` are now using `singer.decimal` [#88](https://github.com/singer-io/tap-shopify/pull/88)

## 1.2.7
  * Change how exceptions are logged to make the error messages more consistent [#84](https://github.com/singer-io/tap-shopify/pull/84)

## 1.2.6
  * Accepts any string for `accepts_marketing_updated_at` field on the `customers` stream [#69](https://github.com/singer-io/tap-shopify/pull/69)

## 1.2.5
  * Bumps `singer-python` from `5.4.1` to `5.9.1` [#67](https://github.com/singer-io/tap-shopify/pull/67)

## 1.2.4
  * Adds `accepts_marketing_updated_at` to shared `customer` schema [#61](https://github.com/singer-io/tap-shopify/pull/61/)

## 1.2.3
  * Bumped Shopify API version to 2020-07 (SDK version 8.0.1) [#63](https://github.com/singer-io/tap-shopify/pull/63/)

## 1.2.2
  * Fixes issue where `products` returns 0 records due to a change in the `status` parameter, Shopify now requires `published_status` [#59](https://github.com/singer-io/tap-shopify/pull/59)

## 1.2.1
  * Update the line_item schema to allow an object under the properties key [#58](https://github.com/singer-io/tap-shopify/pull/58)

## 1.2.0
  * Bump ShopifyAPI version from 3.1.0 -> 7.0.1 [#54](https://github.com/singer-io/tap-shopify/pull/54/)
  * Explicitly specify Shopify API version as `2019-10` [#54](https://github.com/singer-io/tap-shopify/pull/54/)

## 1.1.17
  * Use try/except around JSON metafield's data and fallback to string [#50](https://github.com/singer-io/tap-shopify/pull/50)

## 1.1.16
 * Handles null/non-integer values for `results_per_page` [#48](https://github.com/singer-io/tap-shopify/pull/48)

## 1.1.15
 * Lowering the page size from 250 to 175 as per recommendation from
   Shopify to mitigate receiving 500s [#45](https://github.com/singer-io/tap-shopify/pull/45)

## 1.1.14
 * Bump minimum value for numbers to 1e-10 [commit](https://github.com/singer-io/tap-shopify/commit/a2abf49be96b07f80610d63c514241f829780dcf)

## 1.1.12
 * Canonicalize `transaction__receipt__version` to always prefer and coerce to `version` [#41](https://github.com/singer-io/tap-shopify/pull/41)

## 1.1.11
 * Add `http_request_timer` metrics to HTTP requests [#39](https://github.com/singer-io/tap-shopify/pull/39)

## 1.1.10
 * Canonicalize `transaction__receipt__token` to always use `token` instead of `Token` [#37](https://github.com/singer-io/tap-shopify/pull/37)

## 1.1.9
  * Retry pattern will now fall back to lowercase if `Retry-After` not present [#35](https://github.com/singer-io/tap-shopify/pull/35)

## 1.1.8
  * Uses patternProperties to match extra fields on transactions receipts [#33](https://github.com/singer-io/tap-shopify/pull/33)

## 1.1.7
  * Add `results_per_page` as a config param and allow float values for `date_window_size` [#30](https://github.com/singer-io/tap-shopify/pull/30)

## 1.1.6
  * Fix bookmark resetting for `since_id` after date window finishes [#29](https://github.com/singer-io/tap-shopify/pull/29)

## 1.1.5
  * Check for `updated_at` field in `collect` records.  If absent, sync it [#28](https://github.com/singer-io/tap-shopify/pull/28)

## 1.1.4
  * Update the JSON Schema for "number" elements to use a higher "multipleOf" precision [#27](https://github.com/singer-io/tap-shopify/pull/27)

## 1.1.3
  * Reset local `since_id` to 1 after the date window finishes on normal syncs [#25](https://github.com/singer-io/tap-shopify/pull/25)

## 1.1.2
  * Sets the default paging window to 1 day at a time to account for large volumes [#24](https://github.com/singer-io/tap-shopify/pull/24)

## 1.1.1
  * Updates bookmarking code to capture the since_id in case an interruption occurs while syncing a window with large amounts of data [#23](https://github.com/singer-io/tap-shopify/pull/23)

## 1.1.0
  * Updates a number of schema fields to validate and load as Decimals [#22](https://github.com/singer-io/tap-shopify/pull/22)

## 1.0.4
  * Uses anyOf schema for the shared line_item id [#21](https://github.com/singer-io/tap-shopify/pull/21)

## 1.0.3
  * Adds more retry logic for JSON errors [#19](https://github.com/singer-io/tap-shopify/pull/19)
  * Sets the line_item definition's id field to a number [#20](https://github.com/singer-io/tap-shopify/pull/20)

## 1.0.2
  * Fixes some retry logic to retry JSON Decode errors from weird Shopify responses [#18](https://github.com/singer-io/tap-shopify/pull/18)

## 1.0.1
  * Updates the orders and order_refunds schemas [#17](https://github.com/singer-io/tap-shopify/pull/17)

## 1.0.0
  * Releasing a beta for more general availability

## 0.4.2
  * Reduce query window to 1 week

## 0.4.1
  * Reduce query window to 1 month

## 0.4.0
  * Change bookmarking to use query windows `updated_at_max` as bookmark [#9](https://github.com/singer-io/tap-shopify/pull/9)
