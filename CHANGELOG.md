# Changelog

## 1.1.5
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
