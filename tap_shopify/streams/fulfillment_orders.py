import json
import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class FulfillmentOrders(Stream):
    """Stream class for Shopify fulfillment_orders."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    def transform_childitems(self, data, key, next_page_key):
        """
        Paginate child items.
        """
        child_records = [
            node for item in data["edges"]
            if (node := item.get("node"))
        ]

        # Handle pagination
        page_info = data[key].get("pageInfo", {})
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{data['id'].split('/')[-1]}",
                next_page_key: page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params, query=query)
            node = response.get("edges", [])[0].get("node", {})
            child_records.extend(
                node for item in node[key]["edges"]
                if (node := item.get("node"))
            )
            page_info = node.get("pageInfo", {})

        return child_records

    def transform_object(self, obj):
        """
        Transforms a collection object.
        Args:
            obj (dict): Collection object.
        Returns:
            dict: Transformed collection object.
        """
        if obj.get("merchantRequests"):
            obj["merchantRequests"] = self.transform_childitems(obj.get("merchantRequests"), "merchantRequests", "merchant_request_after")

        if obj.get("locationsForMove"):
            obj["locationsForMove"] = self.transform_childitems(obj.get("locationsForMove"), "locationsForMove", "locations_move_after")
            obj["locationsForMove"]["availableLineItems"] = [node for item in obj["locationsForMove"]["availableLineItems"]["edges"] if (node := item.get("node"))]
            obj["locationsForMove"]["unavailableLineItems"] = [node for item in obj["locationsForMove"]["unavailableLineItems"]["edges"] if (node := item.get("node"))]

        if obj.get("fulfillments"):
            obj["fulfillments"] = self.transform_childitems(obj.get("fulfillments"), "fulfillments", "fulfillments_after")
            obj["fulfillments"]["fulfillmentOrders"] = [node for item in obj["fulfillments"]["fulfillmentOrders"]["edges"] if (node := item.get("node"))]
            obj["fulfillments"]["fulfillmentLineItems"] = [node for item in obj["fulfillments"]["fulfillmentLineItems"]["edges"] if (node := item.get("node"))]
            obj["fulfillments"]["events"] = [node for item in obj["fulfillments"]["events"]["edges"] if (node := item.get("node"))]

        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching orders.
        Returns:
            str: GraphQL query string.
        """
        return """
            query fulfillmentOrders($after: String, $query: String, $merchant_request_after: String, $locations_move_after: String, $fulfillments_after: String) {
                fulfillmentOrders(first: 20, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                        node {
                            id
                            orderId
                            updatedAt
                            supportedActions {
                                action
                                externalUrl
                            }
                            status
                            requestStatus
                            orderProcessedAt
                            orderName
                            channelId
                            fulfillAt
                            fulfillBy
                            createdAt
                            destination {
                                address1
                                address2
                                city
                                countryCode
                                company
                                email
                                firstName
                                id
                                lastName
                                province
                                phone
                                zip
                                location {
                                    id
                                }
                            }
                            fulfillmentHolds {
                                displayReason
                                handle
                                heldByRequestingApp
                                id
                                reason
                                reasonNotes
                                heldByApp {
                                    id
                                }
                            }
                            internationalDuties {
                                incoterm
                            }
                            deliveryMethod {
                                id
                                maxDeliveryDateTime
                                methodType
                                minDeliveryDateTime
                                presentedName
                                serviceCode
                                sourceReference
                                brandedPromise {
                                    handle
                                    name
                                }
                                additionalInformation {
                                    instructions
                                    phone
                                }
                            }
                            assignedLocation {
                                address1
                                address2
                                city
                                countryCode
                                name
                                phone
                                province
                                zip
                                location {
                                    updatedAt
                                    id
                                }
                            }
                            merchantRequests(first: 3, after: $merchant_request_after) {
                                edges {
                                    node {
                                        id
                                        kind
                                        message
                                        requestOptions
                                        responseData
                                        sentAt
                                        fulfillmentOrder {
                                            id
                                        }
                                    }
                                }
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                            }
                            locationsForMove(first: 3, after: $locations_move_after) {
                                edges {
                                    node {
                                        message
                                        movable
                                        location {
                                            id
                                        }
                                        unavailableLineItemsCount {
                                            count
                                            precision
                                        }
                                        availableLineItemsCount {
                                            count
                                            precision
                                        }
                                        availableLineItems(first: 250) {
                                            edges {
                                                node {
                                                    id
                                                }
                                            }
                                            pageInfo {
                                                endCursor
                                                hasNextPage
                                            }

                                        }
                                        unavailableLineItems(first:250) {
                                            edges {
                                                node {
                                                    id
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
                            fulfillments(first: 3, after: $fulfillments_after) {
                                edges {
                                    node {
                                        createdAt
                                        deliveredAt
                                        displayStatus
                                        estimatedDeliveryAt
                                        id
                                        inTransitAt
                                        name
                                        requiresShipping
                                        status
                                        totalQuantity
                                        updatedAt
                                        originAddress {
                                            address1
                                            address2
                                            city
                                            countryCode
                                            provinceCode
                                            zip
                                        }
                                        trackingInfo {
                                            company
                                            number
                                            url
                                        }
                                        service {
                                            id
                                        }
                                        location {
                                            id
                                        }
                                        fulfillmentOrders(first: 250) {
                                            edges {
                                                node {
                                                    id
                                                }
                                            }
                                            pageInfo {
                                                hasNextPage
                                                endCursor
                                            }
                                        }
                                        fulfillmentLineItems(first: 10) {
                                            edges {
                                                node {
                                                    id
                                                    quantity
                                                    originalTotalSet {
                                                        presentmentMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                        shopMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                    }
                                                    discountedTotalSet {
                                                        presentmentMoney {
                                                            amount
                                                            currencyCode
                                                        }
                                                        shopMoney {
                                                            currencyCode
                                                            amount
                                                        }
                                                    }
                                                    lineItem {
                                                        id
                                                    }
                                                }
                                            }
                                            pageInfo {
                                                hasNextPage
                                                endCursor
                                            }
                                        }
                                        events(first: 10) {
                                            edges {
                                                node {
                                                    id
                                                }
                                            }
                                            pageInfo {
                                                endCursor
                                                hasNextPage
                                            }
                                        }
                                        merchantRequests(first: 250) {
                                            edges {
                                                node {
                                                    id
                                                    kind
                                                    message
                                                    requestOptions
                                                    responseData
                                                    sentAt
                                                    fulfillmentOrder {
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


Context.stream_objects["fulfillment_orders"] = FulfillmentOrders
