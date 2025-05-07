import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class Fulfillmentorders(Stream):
    """Stream class for Shopify fulfillment_orders."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    def transform_childitems(self, data, key):
        """
        Transforms the order lineitems data by extracting order IDs and handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of lineItems.
        """
        children = [
            node for item in data["edges"]
            if (node := item.get("node"))
        ]

        # # Handle pagination
        # page_info = data[key].get("pageInfo", {})
        # while page_info.get("hasNextPage"):
            # params = {
            #     "first": self.results_per_page,
            #     "query": f"id:{data['id'].split('/')[-1]}",
            #     "childafter": page_info.get("endCursor"),
            # }

            # # Fetch the next page of data
            # response = self.call_api(params)
            # lineitems_data = response.get("edges", [])[0].get("node", {})
            # lineitems.extend(
            #     node for item in lineitems_data["lineItems"]["edges"]
            #     if (node := item.get("node"))
            # )
            # page_info = lineitems_data.get("pageInfo", {})

        return children

    def transform_object(self, obj):
        """
        Transforms a collection object.

        Args:
            obj (dict): Collection object.

        Returns:
            dict: Transformed collection object.
        """
        if obj.get("merchantRequests"):
            obj["merchantRequests"] = self.transform_childitems(obj.get("merchantRequests"), "merchantRequests")

        if obj.get("locationsForMove"):
            obj["locationsForMove"] = self.transform_childitems(obj.get("locationsForMove"), "locationsForMove")

        if obj.get("fulfillments"):
            obj["fulfillments"] = self.transform_childitems(obj.get("fulfillments"), "fulfillments")

        # LOGGER.info("merchantRequests %s", obj["merchantRequests"])
        LOGGER.info("locationsForMove %s", obj["locationsForMove"])
        # LOGGER.info("fulfillments %s", obj["fulfillments"])

        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching orders.

        Returns:
            str: GraphQL query string.
        """
        return """
            query fulfillmentOrders($after: String, $query: String,) {
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
                            merchantRequests(first: 10) {
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
                            locationsForMove(first: 10) {
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
                                        availableLineItems(first: 10) {
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
                                        unavailableLineItems(first: 10) {
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
                            }
                            fulfillments(first: 10) {
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
                                        fulfillmentOrders(first: 10) {
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
                                                    zip
                                                    status
                                                    province
                                                    message
                                                    longitude
                                                    latitude
                                                    id
                                                    happenedAt
                                                    estimatedDeliveryAt
                                                    createdAt
                                                    country
                                                    address1
                                                    city
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


Context.stream_objects["fulfillment_orders"] = Fulfillmentorders
