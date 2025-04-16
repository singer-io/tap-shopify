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

    def transform_lineitems(self, data):
        """
        Transforms the order lineitems data by extracting order IDs and handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of refunds with lineitems.
        """

        lineitems = [
            node for item in data["refundLineItems"]["edges"]
            if (node := item.get("node"))
        ]

        # Handle pagination
        page_info = data["refundLineItems"].get("pageInfo", {})
        order_parent = data.get("order").get("id")
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{order_parent.split('/')[-1]}",
                "childafter": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            nodes = response.get("edges", [])[0].get("node", {})
            lineitems_data = nodes.get("refunds")[0]
            lineitems.extend(
                node for item in lineitems_data["refundLineItems"]["edges"]
                if (node := item.get("node"))
            )
            page_info = lineitems_data.get("pageInfo", {})

        return lineitems

    def transform_orderadjustments(self, data):
        """
        Transforms the order adjustments data by extracting order IDs and handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of adjustments.
        """

        orderadjustments = [
            node for item in data["orderAdjustments"]["edges"]
            if (node := item.get("node"))
        ]

        # Handle pagination
        page_info = data["orderAdjustments"].get("pageInfo", {})
        order_parent = data.get("order").get("id")
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{order_parent.split('/')[-1]}",
                "orderadjustments_after": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            nodes = response.get("edges", [])[0].get("node", {})
            refunds_data = nodes.get("refunds")[0]
            orderadjustments.extend(
                node for item in refunds_data["orderAdjustments"]["edges"]
                if (node := item.get("node"))
            )
            page_info = refunds_data.get("pageInfo", {})

        return orderadjustments

    def transform_object(self, obj):
        """
        Transform refund objects by extracting refund line items from edges.

        Args:
            obj (dict): Refund object.

        Returns:
            dict: Transformed refund object.
        """

        if obj.get("refundLineItems"):
            obj["refundLineItems"] = self.transform_lineitems(obj)

        if obj.get("orderAdjustments"):
            obj["orderAdjustments"] = self.transform_orderadjustments(obj)
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
        # pylint: disable=line-too-long
        return """query GetOrderRefunds($first: Int!, $after: String, $query: String, $childafter: String, $orderadjustments_after: String) {
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
                                    orderAdjustments(first: 100, after: $orderadjustments_after) {
                                        edges {
                                            node {
                                                amountSet {
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
                                                reason
                                                taxAmountSet {
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
                                        pageInfo {
                                            endCursor
                                            hasNextPage
                                        }
                                    }
                                    refundLineItems(first: 50, after: $childafter) {
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
                                                location {
                                                    id
                                                }
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
                                                lineItem {
                                                    id
                                                    vendor
                                                    quantity
                                                    title
                                                    requiresShipping
                                                    originalTotalSet {
                                                        presentmentMoney {
                                                            currencyCode
                                                            amount
                                                        }
                                                        shopMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                    }
                                                    taxLines(first: 250) {
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
                                                        title
                                                        source
                                                        channelLiable
                                                    }
                                                    taxable
                                                    isGiftCard
                                                    name
                                                    discountedTotalSet {
                                                        presentmentMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                        shopMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                    }
                                                    sku
                                                    product {
                                                        id
                                                    }
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
                                                            index
                                                            targetType
                                                            targetSelection
                                                            allocationMethod
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
                                                    customAttributes {
                                                        key
                                                        value
                                                    }
                                                    totalDiscountSet {
                                                        presentmentMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                        shopMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                    }
                                                    duties {
                                                        harmonizedSystemCode
                                                        id
                                                        taxLines {
                                                            rate
                                                            source
                                                            title
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
                                                        }
                                                        countryCodeOfOrigin
                                                    }
                                                    discountedUnitPriceSet {
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
                                        pageInfo {
                                            hasNextPage
                                            endCursor
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
