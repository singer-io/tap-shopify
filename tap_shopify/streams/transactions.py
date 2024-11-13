import shopify
import singer
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      shopify_error_handling)
import os
import sys
import json

LOGGER = singer.get_logger()

# https://help.shopify.com/en/api/reference/orders/transaction An
# order can have no more than 100 transactions associated with it.
TRANSACTIONS_RESULTS_PER_PAGE = 100

# We have observed transactions with receipt objects that contain both:
#   - `token` and `Token`
#   - `version` and `Version`
#   - `ack` and `Ack`
# keys transactions where PayPal is the payment type. We reached out to
# PayPal support and they told us the values should be the same, so one
# can be safely ignored since its a duplicate. Example: The logic is to
# prefer `token` if both are present and equal, convert `Token` -> `token`
# if only `Token` is present, and throw an error if both are present and
# their values are not equal
def canonicalize(transaction_dict, field_name):
    field_name_upper = field_name.capitalize()
    # Not all Shopify transactions have receipts. Facebook has been shown
    # to push a null receipt through the transaction
    receipt = transaction_dict.get('receipt', {})
    if receipt:
        value_lower = receipt.get(field_name)
        value_upper = receipt.get(field_name_upper)
        if value_lower and value_upper:
            if value_lower == value_upper:
                # LOGGER.info(
                #     "Transaction (id=%d) contains a receipt "
                #     "that has `%s` and `%s` keys with the same "
                #     "value. Removing the `%s` key.",
                #             transaction_dict['id'],
                #             field_name,
                #             field_name_upper,
                #             field_name_upper)
                transaction_dict['receipt'].pop(field_name_upper)
            else:
                raise ValueError((
                    "Found Transaction (id={}) with a receipt that has "
                    "`{}` and `{}` keys with the different "
                    "values. Contact Shopify/PayPal support.").format(
                        transaction_dict['id'],
                        field_name_upper,
                        field_name))
        elif value_upper:
            # pylint: disable=line-too-long
            transaction_dict["receipt"][field_name] = transaction_dict['receipt'].pop(field_name_upper)

class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class Transactions(Stream):
    name = 'transactions'
    replication_key = 'created_at'
    replication_object = shopify.Transaction
    # Transactions have no updated_at property. Therefore we have
    # nothing to set the `replication_method` member to.
    # https://help.shopify.com/en/api/reference/orders/transaction#properties

    gql_query = """
    query Orders($query: String, $cursor: String) {
        orders(first: 250, query: $query, after: $cursor) {
            nodes {
                transactions(first: 100) {
                    authorizationCode
                    createdAt
                    errorCode
                    gateway
                    id
                    kind
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
                    }
                    receiptJson
                    status
                    test
                    parentTransaction {
                        id
                    }
                    amountV2 {
                        amount
                        currencyCode
                    }
                }
                id
            }
            pageInfo {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
            }
        }
    }
    """
    # TODO: add user.id back if they are Shopify Plus store
    # user {
    #     id
    # }
    # add retailLocation.id back on newer API version Field 'retailLocation' doesn't exist on type 'Order'
    # retailLocation {
    #                 id
    #             }

    @shopify_error_handling
    def call_api_for_transactions(self, gql_client, query, cursor=None):
        with HiddenPrints():
            response = gql_client.execute(self.gql_query, dict(query=query, cursor=cursor))
        result = json.loads(response)
        if result.get("errors"):
            raise Exception(result['errors'])
        return result


    def get_transactions(self, query):
        # We do not need to support paging on this substream. If that
        # were to become untrue, reference Metafields.
        #
        # We do not user the `transactions` method of the order object
        # like in metafield because they overrode it here to not
        # support limit overrides.
        #
        # https://github.com/Shopify/shopify_python_api/blob/e8c475ccc84b1516912b37f691d00ecd24921e9b/shopify/resources/order.py#L17-L18

        gql_client = shopify.GraphQL()
        page = self.call_api_for_transactions(gql_client, query)
        yield page

        # paginate
        page_info = page['data']['orders']['pageInfo']
        while page_info['hasNextPage']:
            page = self.call_api_for_transactions(gql_client, query, cursor=page_info['endCursor'])
            page_info = page['data']['orders']['pageInfo']
            yield page

    def get_objects(self):
        # Right now, it's ok for the user to select 'transactions' but not
        # 'orders'. This data may not be all that useful but we're taking
        # the less opinionated approach to begin with to favor simplicity.
        # This is where you would need to add the behavior for enforcing
        # that 'orders' is selected if we want to go that route in the
        # future.

        # Get transactions, bookmarking at `transaction_orders`
        # get the bookmark from the orders stream
        selected_parent = Context.stream_objects['orders']()
        selected_parent.name = "transaction_orders"
        updated_at = selected_parent.get_bookmark().isoformat()
        query = f"updated_at:>'{updated_at}'"

        for page in self.get_transactions(query):
            for order in page['data']['orders']['nodes']:
                order_id = int(order['id'].split("/")[-1])
                location_id = order.get("retailLocation", {}).get("id")
                for raw_tran in order['transactions']:
                    transaction = {
                        "order_id": order_id,
                        "location_id": location_id,
                        "error_code": raw_tran['errorCode'],
                        "user_id": (raw_tran.get("user") or {}).get("id"), # NOTE need to add the missing part of query above for this to work
                        "parent_id": int(raw_tran["parentTransaction"]["id"].split("/")[-1]) if raw_tran.get("parentTransaction") else None,
                        "test": raw_tran['test'],
                        "kind": raw_tran['kind'],
                        "amount": raw_tran['amountV2']['amount'],
                        "currency": raw_tran['amountV2']['currencyCode'],
                        "authorization": raw_tran['authorizationCode'],
                        "gateway": raw_tran['gateway'],
                        "id": raw_tran['id'].split("/")[-1],
                        "created_at": raw_tran['createdAt'],
                        "status": raw_tran['status'],
                        "admin_graphql_api_id": raw_tran['id'],
                        "receipt": json.loads(raw_tran['receiptJson']) if raw_tran.get("receiptJson") else None
                    }

                    # add payment details
                    if raw_tran.get("paymentDetails"):
                        payment_details = raw_tran['paymentDetails']
                        transaction["payment_details"] = {
                            "cvv_result_code": payment_details.get("cvvResultCode"),
                            "credit_card_bin": payment_details.get("bin"),
                            "credit_card_company": payment_details.get("name"),
                            "credit_card_number": payment_details.get("number"),
                            "avs_result_code": payment_details.get("avsResultCode"),
                        }

                    yield transaction


    def sync(self):
        bookmark = self.get_bookmark()
        max_bookmark = bookmark
        for transaction_dict in self.get_objects():
            replication_value = strptime_to_utc(transaction_dict[self.replication_key])
            if replication_value >= bookmark:
                for field_name in ['token', 'version', 'ack', 'timestamp', 'build']:
                    canonicalize(transaction_dict, field_name)
                yield transaction_dict

            if replication_value > max_bookmark:
                max_bookmark = replication_value

        self.update_bookmark(strftime(max_bookmark))

Context.stream_objects['transactions'] = Transactions
