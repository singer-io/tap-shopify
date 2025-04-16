from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Fulfillmentorders(Stream):
    """Stream class for Shopify fulfillment_orders."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    def transform_lineitems(self, data):
        """
        Transforms the order lineitems data by extracting order IDs and handling pagination.

        Args:
            data (dict): Order data.

        Returns:
            list: List of lineItems.
        """
        lineitems = [
            node for item in data["lineItems"]["edges"]
            if (node := item.get("node"))
        ]

        # Handle pagination
        page_info = data["lineItems"].get("pageInfo", {})
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{data['id'].split('/')[-1]}",
                "childafter": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            lineitems_data = response.get("edges", [])[0].get("node", {})
            lineitems.extend(
                node for item in lineitems_data["lineItems"]["edges"]
                if (node := item.get("node"))
            )
            page_info = lineitems_data.get("pageInfo", {})

        return lineitems

    def transform_object(self, obj):
        """
        Transforms a collection object.

        Args:
            obj (dict): Collection object.

        Returns:
            dict: Transformed collection object.
        """
        if obj.get("lineItems"):
            obj["lineItems"] = self.transform_lineitems(obj)
        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching orders.

        Returns:
            str: GraphQL query string.
        """
        return """
            query fulfillmentOrders($first: Int!, $after: String, $query: String,) {
                fulfillmentOrders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
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
                            fulfillments(first: 250) {
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
                                            LocationId: id
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
