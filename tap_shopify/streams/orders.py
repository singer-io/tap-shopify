from datetime import timedelta
import json
import time
import requests
import shopify
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
import singer
from singer import metrics, utils
from tap_shopify.exceptions import ShopifyError

LOGGER = singer.get_logger()
class Orders(Stream):
    name = "orders"
    data_key = "orders"
    replication_key = "updatedAt"

    def get_query(self):
        """
        Returns the GraphQL query string for the bulk operation.
        The date filters will be injected via the bulk operation variables.
        """
        return """
        {
          orders(query: "%s") {
            edges {
              node {
                id
                updatedAt
                createdAt
                name
                fulfillments {
                  trackingNumbers
                  createdAt
                  location {
                    id
                  }
                }
                lineItems {
                  edges {
                    node {
                      id
                      title
                      quantity
                      originalUnitPriceSet {
                        shopMoney {
                          amount
                          currencyCode
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

    def build_query_filter(self, updated_at_min, updated_at_max):
        return f"updated_at:>='{updated_at_min}' AND updated_at:<'{updated_at_max}'"

    def submit_bulk_query(self, query_string):
        operation = {
            "query": """
                mutation bulkOperationRunQuery($query: String!) {
                  bulkOperationRunQuery(query: $query) {
                    bulkOperation {
                      id
                      status
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
            """,
            "variables": {
                "query": query_string
            }
        }
        return shopify.GraphQL().execute(**operation)

    def poll_bulk_completion(self, timeout=900):
        start = time.time()
        wait = 10
        while time.time() - start < timeout:
            response = json.loads(shopify.GraphQL().execute(query="""
                {
                currentBulkOperation {
                    id
                    status
                    errorCode
                    createdAt
                    completedAt
                    objectCount
                    fileSize
                    url
                }
                }
            """))

            if not isinstance(response, dict):
                raise ShopifyError(f"Unexpected GraphQL response: {response}")

            op = response.get("data", {}).get("currentBulkOperation")

            if not isinstance(op, dict):
                raise ShopifyError(f"Unexpected bulk operation format: {op}")

            if op.get("status") == "COMPLETED":
                return op.get("url")
            elif op.get("status") in ["FAILED", "CANCELED"]:
                raise Exception(f"Bulk operation failed: {op.get('errorCode')}")

            time.sleep(wait)
            wait = min(wait * 1.5, 60)

        raise Exception("Timed out waiting for bulk operation.")

    def parse_bulk_jsonl(self, url):
        resp = requests.get(url, stream=True, timeout=self.request_timeout)
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            yield self.transform_object(json.loads(line))

    def transform_object(self, obj):
        if obj.get("lineItems", {}).get("edges"):
            obj["lineItems"] = [item["node"] for item in obj["lineItems"]["edges"]]
        return obj

    def get_objects(self):
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)

            with metrics.http_request_timer(self.name):
                query_filter = self.build_query_filter(
                    utils.strftime(last_updated_at),
                    utils.strftime(query_end)
                )
                query = self.get_query() % query_filter
                LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))
                self.submit_bulk_query(query)
                url = self.poll_bulk_completion()

            for obj in self.parse_bulk_jsonl(url):
                replication_value = utils.strptime_to_utc(obj[self.replication_key])
                current_bookmark = max(current_bookmark, replication_value)
                yield obj

            last_updated_at = query_end
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

Context.stream_objects["orders"] = Orders
