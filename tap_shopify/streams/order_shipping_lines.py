from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class OrderShippingLines(Stream):
    """Stream class for fetching order shippingLines from Shopify"""

    name = "order_shipping_lines"
    data_key = "orders"
    child_data_key = "shippingLines"
    replication_key = "updatedAt"

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Fetch order shipping lines objects within date windows, yielding each shipping line.

        Yields:
            dict: Transformed shipping line object.
        """

        # Set the initial last updated time to the bookmark minus one minute
        # to ensure we don't miss any updates as its observed shopify updates
        # the parent object initially and then the child objects
        last_updated_at = self.get_bookmark() - timedelta(minutes=1)
        current_bookmark = self.get_bookmark()
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

                # Process parent objects and their shippinglines
                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node", {})

                    for shipping_line in self.paginate_shipping_lines(node):
                        shipping_line["orderId"] = node["id"].split("/")[-1]
                        shipping_line["updatedAt"] = node["updatedAt"]

                        replication_value = utils.strptime_with_tz(
                            shipping_line[self.replication_key]
                        )
                        current_bookmark = max(current_bookmark, replication_value)
                        yield self.transform_object(shipping_line)

                # Handle pagination
                page_info = data.get("pageInfo", {})
                cursor = page_info.get("endCursor")
                if not page_info.get("hasNextPage", False):
                    break

            last_updated_at = query_end
            # Update bookmark to the latest value, but not beyond sync start time
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

    def paginate_shipping_lines(self, data):
        """
        Transforms the shippingLines data by handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of shippingLines.
        """

        for item in data["shippingLines"]["edges"]:
            node = item.get("node")
            if node:
                yield node

        # Handle pagination
        page_info = data.get("pageInfo", {})
        order_parent = data["id"]
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{order_parent.split('/')[-1]}",
                "childafter": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            node = response.get("edges", [])[0].get("node", {})
            shipping_lines_data = node.get("shippingLines")
            for item in shipping_lines_data["edges"]:
                node = item.get("node")
                if node:
                    yield node

            page_info = shipping_lines_data.get("pageInfo", {})

    def get_query(self):
        """
        Returns the GraphQL query for fetching order shipping lines.

        Returns:
            str: GraphQL query string.
        """
        return """
            query GetShippingLines($first: Int!, $after: String, $query: String, $childafter: String) {
                orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                        node {
                            id
                            updatedAt
                            shippingLines(first: $first, after: $childafter) {
                                edges {
                                    node {
                                        carrierIdentifier
                                        code
                                        currentDiscountedPriceSet {
                                            presentmentMoney {
                                                amount
                                                currencyCode
                                            }
                                            shopMoney {
                                                amount
                                                currencyCode
                                            }
                                        }
                                        custom
                                        deliveryCategory
                                        discountAllocations {
                                            allocatedAmountSet {
                                                presentmentMoney {
                                                    amount
                                                    currencyCode
                                                }
                                                shopMoney {
                                                    amount
                                                    currencyCode
                                                }
                                            }
                                            discountApplication {
                                                allocationMethod
                                                index
                                                targetSelection
                                                targetType
                                                value {
                                                    ... on MoneyV2 {
                                                        __typename
                                                        amount
                                                        currencyCode
                                                    }
                                                    ... on PricingPercentageValue {
                                                        __typename
                                                        percentage
                                                    }
                                                }
                                                ... on AutomaticDiscountApplication {
                                                    __typename
                                                    allocationMethod
                                                    index
                                                    targetSelection
                                                    targetType
                                                    title
                                                    value {
                                                        ... on MoneyV2 {
                                                            __typename
                                                            amount
                                                            currencyCode
                                                        }
                                                        ... on PricingPercentageValue {
                                                            __typename
                                                            percentage
                                                        }
                                                    }
                                                }
                                                ... on DiscountCodeApplication {
                                                    __typename
                                                    allocationMethod
                                                    code
                                                    index
                                                    targetSelection
                                                    targetType
                                                    value {
                                                        ... on MoneyV2 {
                                                            __typename
                                                            amount
                                                            currencyCode
                                                        }
                                                        ... on PricingPercentageValue {
                                                            __typename
                                                            percentage
                                                        }
                                                    }
                                                }
                                                ... on ManualDiscountApplication {
                                                    description
                                                    allocationMethod
                                                    index
                                                    targetSelection
                                                    targetType
                                                    title
                                                    value {
                                                        ... on MoneyV2 {
                                                            __typename
                                                            amount
                                                            currencyCode
                                                        }
                                                        ... on PricingPercentageValue {
                                                            __typename
                                                            percentage
                                                        }
                                                    }
                                                }
                                                ... on ScriptDiscountApplication {
                                                    __typename
                                                    allocationMethod
                                                    index
                                                    targetSelection
                                                    targetType
                                                    title
                                                    value {
                                                        ... on MoneyV2 {
                                                            __typename
                                                            amount
                                                            currencyCode
                                                        }
                                                        ... on PricingPercentageValue {
                                                            __typename
                                                            percentage
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        discountedPriceSet {
                                            presentmentMoney {
                                                amount
                                                currencyCode
                                            }
                                            shopMoney {
                                                amount
                                                currencyCode
                                            }
                                        }
                                        id
                                        isRemoved
                                        originalPriceSet {
                                            presentmentMoney {
                                                amount
                                                currencyCode
                                            }
                                            shopMoney {
                                                amount
                                                currencyCode
                                            }
                                        }
                                        phone
                                        shippingRateHandle
                                        source
                                        taxLines {
                                            channelLiable
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
                                            rate
                                            ratePercentage
                                            source
                                            title
                                        }
                                        title
                                    }
                                }
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        """


Context.stream_objects["order_shipping_lines"] = OrderShippingLines
