from copy import deepcopy
from contextlib import closing
from datetime import timedelta
import hashlib
import json
import sqlite3
import tempfile
import time

import backoff
import requests
import singer
from graphql import parse, print_ast, visit
from graphql.language import (
    ArgumentNode, FieldNode, NameNode, SelectionSetNode, StringValueNode, Visitor,
)
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.exceptions import BulkOperationInProgressError, ShopifyAPIError
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class FulfillmentOrders(Stream):
    """Stream class for Shopify fulfillment_orders."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    def get_bulk_queries(self):
        """Split supported selected fields into bounded bulk query selections."""
        document = parse(self.remove_fields_from_query(Context.get_unselected_fields(self.name)))
        root = document.definitions[0].selection_set.selections[0]
        fields = root.selection_set.selections[0].selection_set.selections[0].selection_set
        groups = (
            {"merchantRequests", "fulfillmentOrdersForMerge"},
            {"fulfillments"},
        )
        connections = {
            "fulfillmentOrders", "merchantRequests", "fulfillmentOrdersForMerge",
            "locationsForMove", "availableLineItems", "unavailableLineItems",
            "fulfillments", "fulfillmentLineItems", "events",
        }

        class BulkSelection(Visitor):
            def enter_field(self, node, *_):
                if node.name.value == "pageInfo":
                    return self.REMOVE
                return None

            def leave_field(self, node, *_):
                if node.name.value == "nodes":
                    return FieldNode(
                        name=NameNode(value="edges"),
                        selection_set=SelectionSetNode(selections=(FieldNode(
                            name=NameNode(value="node"), selection_set=node.selection_set,
                        ),)),
                    )
                if node.name.value in connections:
                    node.arguments = tuple(
                        argument for argument in node.arguments
                        if argument.name.value not in {"first", "after", "sortKey"}
                    )
                    child = node.selection_set.selections[0].selection_set.selections[0]
                    child.selection_set = SelectionSetNode(selections=(
                        *child.selection_set.selections,
                        FieldNode(
                            alias=NameNode(value=f"bulk_{node.name.value}"),
                            name=NameNode(value="__typename"),
                        ),
                    ))
                return None

        queries = []
        all_children = set().union(*groups, {"locationsForMove"})
        for index, group in enumerate(groups):
            selected = tuple(
                field for field in fields.selections
                if field.name.value in {"id", "updatedAt"}
                or field.name.value in group
                or (index == 0 and field.name.value not in all_children)
            )
            if index and not any(field.name.value in group for field in selected):
                continue
            bulk_document = deepcopy(document)
            bulk_operation = bulk_document.definitions[0]
            bulk_operation.variable_definitions = ()
            bulk_root = bulk_operation.selection_set.selections[0]
            bulk_root.selection_set.selections[0].selection_set.selections[0].selection_set = (
                SelectionSetNode(selections=deepcopy(selected))
            )
            bulk_root.arguments = tuple(
                argument for argument in bulk_root.arguments
                if argument.name.value == "includeClosed"
            )
            queries.append(print_ast(visit(bulk_document, BulkSelection())))
        return queries

    @backoff.on_exception(
        backoff.expo, requests.exceptions.RequestException, max_tries=5,
        giveup=lambda error: (
            error.response is not None
            and error.response.status_code < 500
            and error.response.status_code != 429
        ),
    )
    def bulk_request(self, query, variables=None):
        """Use the fulfillment API version for both submission and polling."""
        url = (
            f"https://{Context.config['shop']}.myshopify.com"
            "/admin/api/2026-10/graphql.json"
        )
        for attempt in range(2):
            headers = {
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": (
                    Context.config.get("access_token") or Context.config.get("api_key")
                ),
            }
            with requests.post(
                url, headers=headers, json={"query": query, "variables": variables},
                timeout=self.request_timeout,
            ) as response:
                if response.status_code == 401 and attempt == 0 and Context.client:
                    Context.client.refresh_token()
                    Context.client.reinitialize_session()
                    Context.config["access_token"] = Context.client.access_token
                    continue
                response.raise_for_status()
                result = response.json()
            if not isinstance(result, dict) or result.get("errors"):
                raise ShopifyAPIError(f"Fulfillment GraphQL error: {result}")
            return result.get("data", {})
        raise ShopifyAPIError("Unable to authenticate fulfillment bulk request")

    @backoff.on_exception(
        backoff.expo, BulkOperationInProgressError,
        max_tries=7, factor=10, jitter=None,
    )
    def submit_bulk_query(self, query_string):
        data = self.bulk_request("""
            mutation bulkOperationRunQuery($query: String!) {
                bulkOperationRunQuery(query: $query, groupObjects: true) {
                    bulkOperation { id status }
                    userErrors { field message }
                }
            }
        """, {"query": query_string})
        result = data.get("bulkOperationRunQuery") or {}
        errors = result.get("userErrors") or []
        if errors:
            if any("already in progress" in error.get("message", "").lower()
                   or "maximum" in error.get("message", "").lower()
                   and "bulk" in error.get("message", "").lower()
                   for error in errors):
                raise BulkOperationInProgressError(str(errors))
            raise ShopifyAPIError(f"Fulfillment bulk query error: {errors}")
        operation = result.get("bulkOperation") or {}
        if not operation.get("id"):
            raise ShopifyAPIError(f"Invalid fulfillment bulk response: {data}")
        return operation

    def poll_bulk_completion(self, bulk_op_id, timeout=82800):
        started = time.monotonic()
        while time.monotonic() - started < timeout:
            data = self.bulk_request("""
                query fulfillmentBulkOperation($id: ID!) {
                    bulkOperation(id: $id) {
                        id status errorCode objectCount url
                    }
                }
            """, {"id": bulk_op_id})
            operation = data.get("bulkOperation")
            if not isinstance(operation, dict):
                self.clear_bulk_operation_state()
                raise ShopifyAPIError(f"Fulfillment bulk operation not found: {bulk_op_id}")
            status = operation.get("status")
            LOGGER.info("Fulfillment bulk operation %s: %s", bulk_op_id, status)
            if status == "COMPLETED":
                if not operation.get("url") and int(operation.get("objectCount") or 0):
                    raise ShopifyAPIError(f"Missing fulfillment bulk result URL: {bulk_op_id}")
                return operation.get("url")
            if status in {"FAILED", "CANCELED", "EXPIRED"}:
                bookmark = Context.state.get("bookmarks", {}).get(self.name, {})
                operations = bookmark.get("bulk_operation", {}).get("operations", {})
                for operation_state in operations.values():
                    if operation_state.get("id") == bulk_op_id:
                        operation_state["status"] = status
                singer.write_state(Context.state)
                raise ShopifyAPIError(
                    f"Fulfillment bulk operation {bulk_op_id}: {status}, "
                    f"error: {operation.get('errorCode')}"
                )
            if status not in {"CREATED", "RUNNING", "CANCELING"}:
                raise ShopifyAPIError(f"Unexpected fulfillment bulk status: {status}")
            time.sleep(60)
        raise ShopifyAPIError(f"Fulfillment bulk operation {bulk_op_id} timed out")

    @staticmethod
    def _connection_children(node):
        result = {}
        for field in node.selection_set.selections:
            if field.selection_set:
                edges = next((child for child in field.selection_set.selections
                              if child.name.value == "edges"), None)
                if edges:
                    result[field.name.value] = edges.selection_set.selections[0]
        return result

    @staticmethod
    def _parse_bulk_record(line):
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ShopifyAPIError("Invalid fulfillment bulk JSONL record")
        parent_id = record.pop("__parentId", None)
        markers = [key for key in record if key.startswith("bulk_")]
        if len(markers) != 1:
            raise ShopifyAPIError(f"Missing fulfillment bulk connection marker: {record}")
        field_name = markers[0][5:]
        record.pop(markers[0])
        return record, parent_id, field_name

    def parse_bulk_jsonl(self, url, query):
        """Rebuild one root record at a time using connection markers and parent IDs."""
        root_node = (
            parse(query).definitions[0].selection_set.selections[0]
            .selection_set.selections[0].selection_set.selections[0]
        )
        current = None
        parents = {}
        with requests.get(url, stream=True, timeout=self.request_timeout) as response:
            if response.status_code in {403, 404}:
                self.clear_bulk_operation_state()
            response.raise_for_status()
            for line in response.iter_lines():
                if not line:
                    continue
                record, parent_id, field_name = self._parse_bulk_record(line)
                if parent_id is None:
                    if field_name != self.data_key or not record.get("id"):
                        raise ShopifyAPIError("Invalid fulfillment bulk root record")
                    if current is not None:
                        yield current
                    current = record
                    parents = {}
                    selection = root_node
                else:
                    parent = parents.get(parent_id)
                    if parent is None or field_name not in parent[1]:
                        raise ShopifyAPIError(f"Unknown fulfillment bulk parent: {parent_id}")
                    selection = parent[1][field_name]
                    parent[0][field_name].append(record)
                children = self._connection_children(selection)
                for child_name in children:
                    record[child_name] = []
                if children:
                    parents[record["id"]] = (record, children)
            if current is not None:
                yield current

    def clear_bulk_operation_state(self):
        bookmark = Context.state.get("bookmarks", {}).get(self.name, {})
        bookmark.pop("bulk_operation", None)
        singer.write_state(Context.state)

    def _load_bulk_results(self, database, queries, pending):
        """Run each window query sequentially and merge its records into the database."""
        for index, template in enumerate(queries):
            document = parse(template)
            root = document.definitions[0].selection_set.selections[0]
            root.arguments = (*root.arguments, ArgumentNode(
                name=NameNode(value="query"),
                value=StringValueNode(value=(
                    f"updated_at:>='{pending['window_start']}' AND "
                    f"updated_at:<'{pending['window_end']}'"
                )),
            ))
            query = print_ast(document)
            operation = pending["operations"].get(str(index))
            with metrics.http_request_timer(self.name):
                if not operation or operation.get("status") in {
                    "FAILED", "CANCELED", "EXPIRED",
                }:
                    operation = self.submit_bulk_query(query)
                    pending["operations"][str(index)] = operation
                    singer.write_state(Context.state)
                url = self.poll_bulk_completion(operation["id"])
                operation["status"] = "COMPLETED"
                singer.write_state(Context.state)
            if url:
                for record in self.parse_bulk_jsonl(url, query):
                    existing = database.execute(
                        "SELECT payload, parts FROM records WHERE id = ?",
                        (record["id"],),
                    ).fetchone()
                    if index and not existing:
                        self.clear_bulk_operation_state()
                        raise ShopifyAPIError(
                            "Fulfillment bulk query roots changed; retry window"
                        )
                    merged = json.loads(existing[0]) if existing else {}
                    merged.update(record)
                    database.execute(
                        "INSERT OR REPLACE INTO records VALUES (?, ?, ?)",
                        (record["id"], json.dumps(merged),
                         existing[1] + 1 if existing else 1),
                    )
            database.commit()

    def get_objects(self):
        queries = self.get_bulk_queries()
        query_hash = hashlib.sha256(json.dumps(queries).encode("utf-8")).hexdigest()
        last_updated_at = self.get_bookmark()
        sync_start = utils.now().replace(microsecond=0)
        bookmark = Context.state.setdefault("bookmarks", {}).setdefault(self.name, {})
        pending = bookmark.get("bulk_operation")
        if pending and (
            pending.get("query_hash") != query_hash
            or pending.get("last_date_window") != self.date_window_size
            or pending.get("window_start") != utils.strftime(last_updated_at)
        ):
            self.clear_bulk_operation_state()
            pending = None

        while last_updated_at < sync_start:
            query_end = (
                utils.strptime_with_tz(pending["window_end"]) if pending else
                min(sync_start, last_updated_at + timedelta(days=self.date_window_size))
            )
            if pending is None:
                pending = {
                    "window_start": utils.strftime(last_updated_at),
                    "window_end": utils.strftime(query_end),
                    "last_date_window": self.date_window_size,
                    "query_hash": query_hash,
                    "operations": {},
                }
                bookmark["bulk_operation"] = pending
                singer.write_state(Context.state)
            with tempfile.TemporaryDirectory() as directory:
                with closing(sqlite3.connect(f"{directory}/fulfillment.sqlite")) as database:
                    database.execute(
                        "CREATE TABLE records (id TEXT PRIMARY KEY, payload TEXT, parts INTEGER)"
                    )
                    self._load_bulk_results(database, queries, pending)
                    if database.execute(
                        "SELECT 1 FROM records WHERE parts != ? LIMIT 1", (len(queries),),
                    ).fetchone():
                        self.clear_bulk_operation_state()
                        raise ShopifyAPIError("Fulfillment bulk query roots changed; retry window")
                    for payload, in database.execute("SELECT payload FROM records"):
                        record = json.loads(payload)
                        if "locationsForMove" not in Context.get_unselected_fields(self.name):
                            record["locationsForMove"] = self.get_locations_for_move(record["id"])
                        yield record
            bookmark.pop("bulk_operation", None)
            self.update_bookmark(utils.strftime(query_end))
            last_updated_at = query_end
            pending = None

    def get_locations_for_move(self, fulfillment_order_id):
        """Fetch the non-Node connection without an expensive nested selection."""
        query = """
            query fulfillmentMoveLocations($id: ID!, $first: Int!, $after: String) {
                fulfillmentOrder(id: $id) {
                    locationsForMove(first: $first, after: $after) {
                        nodes {
                            message movable location { id }
                            unavailableLineItemsCount { count precision }
                            availableLineItemsCount { count precision }
                        }
                        pageInfo { hasNextPage endCursor }
                    }
                }
            }
        """
        params = {"id": fulfillment_order_id, "first": min(self.results_per_page, 30)}
        locations = []
        while True:
            response = self.bulk_request(query, params)
            parent = response.get("fulfillmentOrder")
            if not parent:
                raise ShopifyAPIError(f"Fulfillment order not found: {fulfillment_order_id}")
            connection = parent["locationsForMove"]
            for location in connection["nodes"]:
                for field in ("availableLineItems", "unavailableLineItems"):
                    location[field] = self.get_location_line_items(
                        fulfillment_order_id, location["location"]["id"], field,
                    )
                locations.append(location)
            page = connection["pageInfo"]
            if not page["hasNextPage"]:
                return locations
            cursor = page.get("endCursor")
            if not cursor or cursor == params.get("after"):
                raise ShopifyAPIError("Missing or repeated fulfillment location cursor")
            params["after"] = cursor

    def get_location_line_items(self, fulfillment_order_id, location_id, field):
        query = """
            query fulfillmentMoveLineItems($id: ID!, $locationIds: [ID!], $after: String) {
                fulfillmentOrder(id: $id) {
                    locationsForMove(first: 1, locationIds: $locationIds) {
                        nodes {
                            %s(first: 250, after: $after) {
                                nodes { id }
                                pageInfo { hasNextPage endCursor }
                            }
                        }
                    }
                }
            }
        """ % field
        params = {"id": fulfillment_order_id, "locationIds": [location_id]}
        records = []
        while True:
            response = self.bulk_request(query, params)
            parent = response.get("fulfillmentOrder") or {}
            locations = parent.get("locationsForMove", {}).get("nodes", [])
            if not locations:
                raise ShopifyAPIError(f"Fulfillment move location not found: {location_id}")
            connection = locations[0][field]
            records.extend(connection["nodes"])
            page = connection["pageInfo"]
            if not page["hasNextPage"]:
                return records
            cursor = page.get("endCursor")
            if not cursor or cursor == params.get("after"):
                raise ShopifyAPIError("Missing or repeated fulfillment location line-item cursor")
            params["after"] = cursor

    def get_query(self):
        """
        Returns the selection template used to construct fulfillment bulk queries.
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
                                            pageInfo {
                                                    hasNextPage
                                                    endCursor
                                                }
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
