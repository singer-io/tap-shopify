from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Transactions(Stream):
    """Stream class for Shopify transactions."""

    name = "transactions"
    data_key = "orders"
    child_data_key = "transactions"
    replication_key = "createdAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        parent_filter_key = "updated_at"
        query = (
            f"{parent_filter_key}:>='{updated_at_min}' "
            f"AND {parent_filter_key}:<'{updated_at_max}'"
        )
        params = {
            "query": query,
            "first": self.results_per_page,
        }

        if cursor:
            params["after"] = cursor
        return params

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Fetch transaction objects within date windows, yielding each transaction individually.

        Yields:
            dict: Transformed transaction object.
        """
        # Set the initial last updated time to the bookmark minus one minute
        # to ensure we don't miss any updates as its observed shopify updates
        # the parent object initially and then the child objects
        last_updated_at = self.get_bookmark() - timedelta(minutes=1)
        initial_bookmark_time = current_bookmark = self.get_bookmark()
        sync_start = utils.now().replace(microsecond=0)

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)
            cursor = None

            while True:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)

                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                edges = data.get("edges", [])
                for edge in edges:
                    node = edge.get("node", {})
                    child_edges = node.get(self.child_data_key, [])

                    # Yield each transformed transaction object
                    for child_obj in child_edges:
                        replication_value = utils.strptime_with_tz(child_obj[self.replication_key])
                        current_bookmark = max(current_bookmark, replication_value)
                        # Perform the pseudo sync for the child objects
                        if replication_value >= initial_bookmark_time:
                            yield self.transform_object(child_obj)

                page_info = data.get("pageInfo", {})
                cursor = page_info.get("endCursor")
                if not page_info.get("hasNextPage", False):
                    break

            last_updated_at = query_end
            # Update bookmark to the latest value, but not beyond sync start time
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

    def get_query(self):
        """
        Returns query for fetching transactions.

        Note:
            Shopify has a limit of 100 transactions per order as per shopify support.
            To be on the safer side, we are limiting the transactions to 250.

        Returns:
            str: GraphQL query string.
        """
        return """
            query GetTransactions($first: Int!, $after: String, $query: String) {
            orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                edges {
                node {
                    transactions(first: 250) {
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
                    fees {
                        id
                        rate
                        rateName
                        taxAmount {
                        amount
                        currencyCode
                        }
                        type
                        flatFeeName
                        amount {
                        amount
                        currencyCode
                        }
                        flatFee {
                        amount
                        currencyCode
                        }
                    }
                    manuallyCapturable
                    paymentDetails {
                        ... on CardPaymentDetails {
                        avsResultCode
                        bin
                        company
                        cvvResultCode
                        expirationMonth
                        expirationYear
                        name
                        number
                        paymentMethodName
                        wallet
                        }
                        ... on LocalPaymentMethodsPaymentDetails {
                        paymentDescriptor
                        paymentMethodName
                        }
                        ... on ShopPayInstallmentsPaymentDetails {
                        paymentMethodName
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
            }
        """


Context.stream_objects["transactions"] = Transactions
