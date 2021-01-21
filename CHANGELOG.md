# Changelog

## 1.2.7
  * Bumped Shopify API version to 2021-01 (SDK version 8.2.0) [#79](https://github.com/singer-io/tap-shopify/pull/79/)

## 1.2.6
  * Accepts any string for `accepts_marketing_updated_at` field on the `customers` stream [#69] (https://github.com/singer-io/tap-shopify/pull/69)

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
