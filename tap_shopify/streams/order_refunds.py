from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class OrderRefunds(Stream):
    """Stream class for fetching order refunds from Shopify"""

    name = "order_refunds"
    data_key = "orders"
    child_data_key = "refunds"
    replication_key = "updatedAt"

    def get_objects(self):
        """
        Fetch order refund objects within date windows, yielding each refund individually.

        Yields:
            dict: Transformed refund object.
        """
        # Will always fetch the data from the start date and filter it in the code
        # Shopify doesn't support filtering by updated_at for metafields
        last_updated_at = utils.strptime_with_tz(Context.config["start_date"])
        sync_start = utils.now().replace(microsecond=0)

        # Process each date window
        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)
            cursor = None

            while True:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)

                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                # Process parent objects and their refunds
                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node", {})
                    child_edges = node.get(self.child_data_key, [])

                    # Yield each transformed refund
                    for child_obj in child_edges:
                        yield self.transform_object(child_obj)

                # Handle pagination
                page_info = data.get("pageInfo", {})
                cursor = page_info.get("endCursor")
                if not page_info.get("hasNextPage", False):
                    break

            last_updated_at = query_end

    def transform_object(self, obj):
        """
        Transform refund objects by extracting refund line items from edges.

        Args:
            obj (dict): Refund object.

        Returns:
            dict: Transformed refund object.
        """
        obj["refundLineItems"] = [
            edge.get("node") for edge in obj.get("refundLineItems", {}).get("edges", [])
        ]
        return obj

    def sync(self):
        """
        Performs pseudo incremental sync.
        """
        start_time = utils.now().replace(microsecond=0)
        max_bookmark_value = current_bookmark_value = self.get_bookmark()

        for obj in self.get_objects():
            replication_value = utils.strptime_to_utc(obj[self.replication_key])

            max_bookmark_value = max(max_bookmark_value, replication_value)

            if replication_value >= current_bookmark_value:
                yield obj

        # Update bookmark to the latest value, but not beyond sync start time
        max_bookmark_value = min(start_time, max_bookmark_value)
        self.update_bookmark(utils.strftime(max_bookmark_value))

    def get_query(self):
        """
        Returns the GraphQL query for fetching order refunds.

        Returns:
            str: GraphQL query string.
        """
        return """query GetOrderRefunds($first: Int!, $after: String, $query: String) {
                    orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                        edges {
                            node {
                                refunds(first: 250) {
                                    id
                                    createdAt
                                    legacyResourceId
                                    note
                                    order {
                                        id
                                    }
                                    refundLineItems(first: 250) {
                                        edges {
                                            node {
                                                id
                                                quantity
                                                priceSet {
                                                    presentmentMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                    shopMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                }
                                                restockType
                                                restocked
                                                subtotalSet {
                                                    presentmentMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                    shopMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                }
                                                totalTaxSet {
                                                    presentmentMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                    shopMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    updatedAt
                                }
                            }
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                    }
                }"""


Context.stream_objects["order_refunds"] = OrderRefunds
