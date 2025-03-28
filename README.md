# tap-shopify

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Shopify Graphql Admin API](https://shopify.dev/docs/api/admin-graphql/latest)
- Extracts the following resources:
  - [Abandoned Checkouts](https://shopify.dev/docs/api/admin-graphql/latest/queries/abandonedcheckouts)
  - [Collections](https://shopify.dev/docs/api/admin-graphql/latest/queries/collections)
  - [Customers](https://shopify.dev/docs/api/admin-graphql/latest/queries/customers)
  - [Metafields Collections](https://shopify.dev/docs/api/admin-graphql/latest/queries/collections)
  - [Metafields Customers](https://shopify.dev/docs/api/admin-graphql/latest/queries/customers)
  - [Metafields Orders](https://shopify.dev/docs/api/admin-graphql/latest/queries/orders)
  - [Metafields Products](https://shopify.dev/docs/api/admin-graphql/latest/queries/products)
  - [Orders](https://shopify.dev/docs/api/admin-graphql/latest/queries/orders)
  - [Products](https://shopify.dev/docs/api/admin-graphql/latest/queries/products)
  - [Product Variants](https://shopify.dev/docs/api/admin-graphql/latest/queries/productVariants)
  - [Transactions](https://shopify.dev/docs/api/admin-graphql/latest/queries/orders)
  - [Locations](https://shopify.dev/docs/api/admin-graphql/latest/queries/locations)
  - [Inventory Levels](https://shopify.dev/docs/api/admin-graphql/latest/queries/inventorylevel)
  - [Inventory Item](https://shopify.dev/docs/api/admin-graphql/latest/queries/inventoryitems)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Stream Details

| Stream Name            | Replication Key | Key Properties |
|------------------------|----------------|---------------|
| abandoned_checkouts    | updatedAt      | id            |
| collections            | updatedAt      | id            |
| customers              | updatedAt      | id            |
| events                 | createdAt      | id            |
| inventory_items        | updatedAt      | id            |
| inventory_levels       | updatedAt      | id            |
| locations              | createdAt      | id            |
| metafields_collections  | updatedAt      | id            |
| metafields_customers   | updatedAt      | id            |
| metafields_orders      | updatedAt      | id            |
| metafields_products    | updatedAt      | id            |
| order_refunds         | updatedAt      | id            |
| orders                 | updatedAt      | id            |
| product_variants      | updatedAt      | id            |
| products               | updatedAt      | id            |
| transactions           | createdAt      | id            |

Currently, `locations` graphql endpoint doesn't support querying on the `updatedAt`, therefore, `createdAt` is made the replication key.

## Quick Start

1. Install

    pip install tap-shopify

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2010-01-01",
        "api_key": "<Shopify API Key>",
        "shop": "test_shop",
        "request_timeout": 300
    }
    ```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `api_key` is the API key for your Shopify shop generated via an OAuth flow.

   The `shop` is your Shopify shop which will be the value `test_shop` in the string `https://test_shop.myshopify.com`

    The `request_timeout` is the timeout for the requests. Default: 300 seconds

4. Run the Tap in Discovery Mode

    tap-shopify -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-shopify -c config.json --catalog catalog-file.json

---

Copyright &copy; 2025 Stitch
