# tap-shopify

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Shopify](https://help.shopify.com/en/api/reference)
- Extracts the following resources:
  - [Abandoned Checkouts](https://help.shopify.com/en/api/reference/orders/abandoned_checkouts)
  - [Collects](https://help.shopify.com/en/api/reference/products/collect)
  - [Custom Collections](https://help.shopify.com/en/api/reference/products/customcollection)
  - [Customers](https://help.shopify.com/en/api/reference/customers)
  - [Metafields](https://help.shopify.com/en/api/reference/metafield)
  - [Orders](https://help.shopify.com/en/api/reference/orders)
  - [Products](https://help.shopify.com/en/api/reference/products)
  - [Transactions](https://help.shopify.com/en/api/reference/orders/transaction)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state
- When Metafields are selected, this tap will sync the Shopify store's top-level Metafields and any additional Metafields for selected tables that also have them (ie: Orders, Products, Customers)

## Quick Start

1. Install

    pip install tap-shopify

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2010-01-01",
        "api_key": "<Shopify API Key>",
        "shop": "test_shop"
    }
    ```

   The `start_date` specifies the date at which the tap will begin pulling data
   (for those resources that support this).

   The `api_key` is the API key for your Shopify shop generated via an OAuth flow.

   The `shop` is your Shopify shop which will be the value `test_shop` in the string `https://test_shop.myshopify.com`

4. Run the Tap in Discovery Mode

    tap-shopify -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-shopify -c config.json --catalog catalog-file.json

---

Copyright &copy; 2019 Stitch
