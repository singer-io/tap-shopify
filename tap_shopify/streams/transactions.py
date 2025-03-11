
from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import DATE_WINDOW_SIZE
from tap_shopify.streams.graphql import ShopifyGqlStream


class Transactions(ShopifyGqlStream):
    name = 'transactions'
    data_key = "orders"
    child_data_key = "transactions"
    replication_key = "createdAt"

    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def get_objects(self):
        """
        Fetch transaction objects within date windows, yielding each transaction individually
        """
        last_updated_at = self.get_bookmark()
        sync_start = utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Process each date window
        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=date_window_size)
            query_end = min(sync_start, date_window_end)
            cursor = None

            while True:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)

                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                # Process parent objects and their transactions
                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node", {})
                    child_edges = node.get(self.child_data_key, [])

                    # Yield each transformed transaction
                    yield from (self.transform_object(child_obj) for child_obj in child_edges)

                # Handle pagination
                page_info = data.get("pageInfo", {})
                cursor = page_info.get("endCursor")
                if not page_info.get("hasNextPage", False):
                    break

            # Move to next date window
            last_updated_at = query_end

    def sync(self):
        """Sync transactions and update bookmarks"""
        start_time = utils.now().replace(microsecond=0)
        max_bookmark_value = current_bookmark_value = self.get_bookmark()

        for obj in self.get_objects():
            replication_value = utils.strptime_to_utc(obj[self.replication_key])

            # Track max bookmark value seen
            if replication_value > max_bookmark_value:
                max_bookmark_value = replication_value

            # Only yield records that are new or updated since the last sync
            if replication_value >= current_bookmark_value:
                yield obj

        # Update bookmark to the latest value, but not beyond sync start time
        max_bookmark_value = min(start_time, max_bookmark_value)
        self.update_bookmark(utils.strftime(max_bookmark_value))

    def get_query(self):
        """
        Returns query for fetching transactions.
        Note - Shopify has a limit of 100 transactions per order.
        """
        qry = """
            query GetTransactions($first: Int!, $after: String, $query: String) {
            orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                edges {
                node {
                    transactions(first: 100) {
                    accountNumber
                    amountRoundingSet {
                        presentmentMoney {
                        amount
                        currencyCode
                        }
                        shopMoney {
                        amount
                        currencyCode
                        }
                    }
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
                    authorizationCode
                    authorizationExpiresAt
                    createdAt
                    errorCode
                    formattedGateway
                    gateway
                    id
                    kind
                    manualPaymentGateway
                    maximumRefundableV2 {
                        amount
                        currencyCode
                    }
                    multiCapturable
                    order {
                        id
                    }
                    parentTransaction {
                        accountNumber
                        createdAt
                        id
                        status
                        paymentId
                        processedAt
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
                    }
                    paymentId
                    processedAt
                    receiptJson
                    settlementCurrency
                    settlementCurrencyRate
                    shopifyPaymentsSet {
                        extendedAuthorizationSet {
                        extendedAuthorizationExpiresAt
                        standardAuthorizationExpiresAt
                        }
                        refundSet {
                        acquirerReferenceNumber
                        }
                    }
                    status
                    test
                    totalUnsettledSet {
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
                endCursor
                hasNextPage
                }
            }
            }"""
        return qry

Context.stream_objects['transactions'] = Transactions
