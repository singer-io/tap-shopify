from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class TenderTransactions(Stream):
    """Stream class for Tender Transactions in Shopify."""
    name = "tender_transactions"
    data_key = "tenderTransactions"
    replication_key = "processedAt"
    access_scope = ["read_orders"]

    def get_query(self):
        return """
        query TenderTransactions($first: Int!, $after: String, $query: String) {
            tenderTransactions(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        paymentMethod
                        processedAt
                        remoteReference
                        test
                        transactionDetails {
                            ... on TenderTransactionCreditCardDetails {
                                creditCardCompany
                                creditCardNumber
                            }
                        }
                        amount {
                            amount
                            currencyCode
                        }
                        order {
                            id
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

Context.stream_objects["tender_transactions"] = TenderTransactions
