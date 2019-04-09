# tap-shopify

## Source Repository
* Git Repo: https://github.com/singer-io/tap-bing-ads
* To Sync Fork: https://help.github.com/articles/syncing-a-fork/

## Information
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
        "start_date": "2019-01-01T00:00:00Z",
        "end_date": "2019-01-31T00:00:00Z",
        "shop": "test_shop",
        "is_private_app": true,
        "api_key": "<<Shopify API Key>>",
        "api_password": "<<Shopify API Password (if private app)>>",
        "use_async": true
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

## Performance

### Shopify Constraints
Shopify API's throttle is designed to allow your app to make unlimited requests at a steady rate over time while also having the capacity to make infrequent bursts. The throttle operates using a [leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) algorithm. The bucket size and leak rate properties determine the API's burst behavior and call rate.

|                 | Bucket Size | Leak Rate  | Max. Results/Call  |
| --------------- |------------:| ----------:| ------------------:|
| Shopify Regular | 40          | 2/second   | 250                |
| Shopify Plus    | 80          | 4/second   | 250                |

### Async Logic
1. I found that using Shopify's SDK is slower by almost a factor of 10 compared to calling the REST endpoints.
    * So, we scrap that, and just make calls directly to the REST endpoints.
2. Check if Shop is Regular or Plus to determine Bucket Size.
3. Check the total number of orders for the date range you are pulling data for.
4. Num. Pages = Total Number of Orders/250
5. Each call you can get one page and you are allowed to make 40 calls (or 80 calls).
    *  Num. orders you can retrieve by making all 40 calls (or 80 calls) asynchronously = 10,000 (or 20,000)

6. Say, total number of orders = 100,000.
    * Then, Num. Pages = 100,000/250 = 400
    * We cannot make 400 calls asynchronously, so we chunk them into 40 calls each, which gives us a list A that contains nested lists of 40 calls.
    * We iterate through list A, and using `asyncio` and `aiohttp` we make 40 asynchronous calls, retrieve the results. Then, move onto the next 40 calls.
    * Then, finally return all the results by page order.


---

Copyright &copy; 2019 Stitch
