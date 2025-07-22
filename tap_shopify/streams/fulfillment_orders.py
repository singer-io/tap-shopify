import singer
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class FulfillmentOrders(Stream):
    """Stream class for Shopify fulfillment_orders."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    # pylint: disable=W0221,fixme
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
        rkey = self.camel_to_snake(self.replication_key)

        params = {
            "query": f"{rkey}:>='{updated_at_min}' AND {rkey}:<'{updated_at_max}'",
            "first": self.results_per_page if self.results_per_page <= 30 else 30,
        }

        if cursor:
            params["after"] = cursor
        return params

    def transform_childitems(self, data, parent_id, key, next_page_key):
        """
        Paginate child items.
        """
        child_records = [
            node for item in data["edges"]
            if (node := item.get("node"))
        ]
        # Handle pagination
        page_info = data.get("pageInfo", {})
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{parent_id.split('/')[-1]}",
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
            obj["merchantRequests"] = self.transform_childitems(
                obj.get("merchantRequests"),
                obj["id"], "merchantRequests",
                "merchant_request_after"
            )

        if obj.get("locationsForMove"):
            obj["locationsForMove"] = self.transform_childitems(
                obj.get("locationsForMove"), obj["id"],
                "locationsForMove",
                "locations_move_after"
            )
            for item in obj["locationsForMove"]:
                item["availableLineItems"] = item["availableLineItems"]["nodes"]
                item["unavailableLineItems"] = item["unavailableLineItems"]["nodes"]

        if obj.get("fulfillments"):
            obj["fulfillments"] = self.transform_childitems(
                obj.get("fulfillments"), obj["id"],
                "fulfillments",
                "fulfillments_after"
            )
            for item in obj["fulfillments"]:
                item["fulfillmentOrders"] = item["fulfillmentOrders"]["nodes"]
                item["fulfillmentLineItems"] = item["fulfillmentLineItems"]["nodes"]
                item["events"] = item["events"]["nodes"]

        if obj.get("fulfillmentOrdersForMerge"):
            obj["fulfillmentOrdersForMerge"] = obj["fulfillmentOrdersForMerge"]["nodes"]

        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching fulfillmentOrders.
        Returns:
            str: GraphQL query string.
        """
        return """
            query fulfillmentOrders($first: Int!, $after: String, $query: String, $merchant_request_after: String, $locations_move_after: String, $fulfillments_after: String) {
                fulfillmentOrders(first: $first, after: $after, query: $query, includeClosed: true, sortKey: UPDATED_AT) {
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
                                            nodes {
                                                id
                                            }
                                        }
                                        unavailableLineItems(first:250) {
                                            nodes {
                                                id
                                            }
                                        }
                                    }
                                }
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                            }
                            fulfillmentOrdersForMerge(first: 250) {
                                nodes {
                                    id
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
                                            nodes {
                                                id
                                            }
                                        }
                                        fulfillmentLineItems(first: 3) {
                                            nodes {
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
                                        legacyResourceId
                                        order {
                                            id
                                        }
                                        events(first: 250) {
                                            nodes {
                                                id
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
