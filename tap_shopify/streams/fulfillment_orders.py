from copy import deepcopy
from contextlib import closing
from datetime import timedelta
import hashlib
import json
import os
import sqlite3
import tempfile
import requests
import singer
from singer import utils
from graphql import parse, print_ast, visit
from graphql.language import FieldNode, NameNode, SelectionSetNode, StringValueNode, Visitor
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyAPIError
from tap_shopify.streams.orders import Orders

LOGGER = singer.get_logger()


class BulkQueryVisitor(Visitor):
    def enter_field(self, node, *_):
        if node.name.value == "pageInfo":
            return None
        node.arguments = tuple(argument for argument in node.arguments or []
                               if argument.name.value not in ("first", "after"))
        if node.name.value == "fulfillmentOrders" and any(
                argument.name.value == "query" for argument in node.arguments):
            for argument in node.arguments:
                if argument.name.value == "query":
                    argument.value = StringValueNode(value="%s")
        if node.name.value == "nodes":
            return FieldNode(name=NameNode(value="edges"), selection_set=SelectionSetNode(
                selections=[FieldNode(name=NameNode(value="node"),
                                      selection_set=node.selection_set)]))
        return node

    def leave_selection_set(self, node, *_):
        return SelectionSetNode(selections=[field for field in node.selections
                                            if field.name.value != "pageInfo"])


class FulfillmentOrders(Orders):
    """Fulfillment orders using the Orders bulk submission and polling lifecycle."""

    name = "fulfillment_orders"
    data_key = "fulfillmentOrders"
    replication_key = "updatedAt"

    def __init__(self):
        super().__init__()
        self._bulk_poll_completed = False
        self._bulk_window_state = {}
        self._bulk_query_index = 0
        self._bulk_query_hash = ""

    @staticmethod
    def _get_record_node(document):
        root = document.definitions[0].selection_set.selections[0]
        return root.selection_set.selections[0].selection_set.selections[0]

    @staticmethod
    def _get_record_fields(document):
        record = FulfillmentOrders._get_record_node(document)
        return record.selection_set.selections

    @staticmethod
    def _get_query_groups(fields):
        fulfillment = next((field for field in fields if field.name.value == "fulfillments"), None)
        groups = [[field for field in fields
                   if field.name.value not in ("locationsForMove", "fulfillments")]]
        if fulfillment:
            fulfillment_fields = FulfillmentOrders._get_record_fields_for_fulfillment(fulfillment)
            for references_only in (False, True):
                child = deepcopy(fulfillment)
                child_record = FulfillmentOrders._get_record_fields_for_fulfillment_node(child)
                child_record.selection_set = SelectionSetNode(selections=[
                    field for field in fulfillment_fields
                    if (field.name.value in ("id", "fulfillmentOrders") if references_only
                        else field.name.value != "fulfillmentOrders")
                ])
                groups.append([FieldNode(name=NameNode(value="id")), child])
        return groups

    @staticmethod
    def _get_record_fields_for_fulfillment(field):
        record = FulfillmentOrders._get_record_fields_for_fulfillment_node(field)
        return record.selection_set.selections

    @staticmethod
    def _get_record_fields_for_fulfillment_node(field):
        return field.selection_set.selections[0].selection_set.selections[0]

    @staticmethod
    def _build_bulk_query(document, group):
        part = deepcopy(document)
        operation = part.definitions[0]
        operation.variable_definitions = ()
        operation.name = None
        record = FulfillmentOrders._get_record_node(part)
        record.selection_set = SelectionSetNode(selections=deepcopy(group))
        return print_ast(visit(part, BulkQueryVisitor()))

    def get_bulk_queries(self):
        """Split selected connections into queries within Shopify's bulk limits."""
        document = parse(self.remove_fields_from_query(Context.get_unselected_fields(self.name)))
        fields = self._get_record_fields(document)
        return [self._build_bulk_query(document, group)
                for group in self._get_query_groups(fields)]

    def update_bookmark(self, bookmark_value, bookmark_key=None, bulk_op_metadata=None):
        if bulk_op_metadata:
            self._bulk_poll_completed = bulk_op_metadata.get("status") == "COMPLETED"
            self._bulk_window_state["operations"][str(self._bulk_query_index)] = bulk_op_metadata
            Context.state.setdefault("bookmarks", {}).setdefault(self.name, {})[
                "bulk_operation"] = deepcopy(self._bulk_window_state)
            singer.write_state(Context.state)
        else:
            super().update_bookmark(bookmark_value, bookmark_key)

    @staticmethod
    def _get_bulk_connection_names(query):
        root = parse(query).definitions[0].selection_set.selections[0]
        fields = FulfillmentOrders._get_record_fields_from_query_root(root)
        connections = {
            field.name.value for field in fields
            if field.selection_set and any(
                child.name.value == "edges" for child in field.selection_set.selections
            )
        }
        fulfillment = next((field for field in fields if field.name.value == "fulfillments"), None)
        nested_connections = set()
        if fulfillment:
            child_fields = FulfillmentOrders._get_record_fields_for_fulfillment(fulfillment)
            nested_connections = {
                field.name.value for field in child_fields
                if field.selection_set and any(
                    child.name.value == "edges" for child in field.selection_set.selections
                )
            }
        return connections, nested_connections

    @staticmethod
    def _get_record_fields_from_query_root(root):
        return root.selection_set.selections[0].selection_set.selections[0].selection_set.selections

    @staticmethod
    def _append_bulk_child(record, parent_id, current, fulfillments,
                           connections, nested_connections):
        record_type = record["id"].split("/")[-2]
        if parent_id == current["id"]:
            connection = {"Fulfillment": "fulfillments",
                          "FulfillmentOrderMerchantRequest": "merchantRequests",
                          "FulfillmentOrder": "fulfillmentOrdersForMerge"}.get(record_type)
            if connection not in connections:
                raise ShopifyAPIError(f"Unexpected bulk child type: {record_type}")
            current[connection].append(record)
            if connection == "fulfillments":
                record.update({nested: [] for nested in nested_connections})
                fulfillments[record["id"]] = record
            return

        connection = {"FulfillmentLineItem": "fulfillmentLineItems",
                      "FulfillmentOrder": "fulfillmentOrders",
                      "Event": "events", "BasicEvent": "events",
                      "CommentEvent": "events"}.get(record_type)
        if parent_id not in fulfillments or connection not in nested_connections:
            raise ShopifyAPIError(f"Unexpected bulk parent or child: {parent_id}, {record_type}")
        fulfillments[parent_id][connection].append(record)

    def parse_fulfillment_bulk_jsonl(self, url, query):
        """Rebuild one parent group at a time using JSONL parent IDs."""
        connections, nested_connections = self._get_bulk_connection_names(query)
        current = None
        fulfillments = {}
        with requests.get(url, stream=True, timeout=self.request_timeout) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if not line:
                    continue
                record = json.loads(line)
                parent_id = record.pop("__parentId", None)
                if parent_id is None:
                    if current is not None:
                        yield current
                    current = record
                    current.update({connection: [] for connection in connections})
                    fulfillments = {}
                    continue
                if current is None:
                    raise ShopifyAPIError("Bulk child appeared before its fulfillment order")
                self._append_bulk_child(record, parent_id, current, fulfillments,
                                        connections, nested_connections)
            if current is not None:
                yield current

    def merge_bulk_record(self, existing, supplement):
        for key, value in supplement.items():
            if key == "fulfillments":
                by_id = {item["id"]: item for item in existing.setdefault(key, [])}
                for item in value:
                    if item["id"] in by_id:
                        by_id[item["id"]].update(item)
                    else:
                        existing[key].append(item)
                        by_id[item["id"]] = item
            else:
                existing[key] = value
        return existing

    def _get_window_records(self, last_updated_at, query_end, queries, stored):
        self._bulk_window_state = stored or {
            "window_start": utils.strftime(last_updated_at),
            "window_end": utils.strftime(query_end),
            "last_date_window": self.date_window_size,
            "query_hash": self._bulk_query_hash,
            "operations": {},
        }
        with tempfile.TemporaryDirectory(prefix="tap-shopify-fulfillments-") as directory:
            db_path = os.path.join(directory, "records.sqlite")
            with closing(sqlite3.connect(db_path)) as database:
                database.execute(
                    "CREATE TABLE records (id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
                )
                for index, query in enumerate(queries):
                    self._bulk_query_index = index
                    self._bulk_poll_completed = False
                    operation = self._bulk_window_state["operations"].get(str(index), {})
                    if operation.get("bulk_operation_id"):
                        url = self.poll_bulk_completion(
                            last_updated_at, operation["bulk_operation_id"])
                    else:
                        url = self.submit_and_poll_bulk_query(
                            query, last_updated_at, query_end, last_updated_at)
                    if url is None and not self._bulk_poll_completed:
                        raise ShopifyAPIError("Fulfillment bulk operation was not found")
                    if url:
                        for record in self.parse_fulfillment_bulk_jsonl(url, query):
                            row = database.execute(
                                "SELECT payload FROM records WHERE id = ?", (record["id"],)
                            ).fetchone()
                            if index and row is None:
                                continue
                            merged = (self.merge_bulk_record(json.loads(row[0]), record)
                                      if row else record)
                            if row:
                                database.execute(
                                    "UPDATE records SET payload = ? WHERE id = ?",
                                    (json.dumps(merged), record["id"])
                                )
                            else:
                                database.execute(
                                    "INSERT INTO records VALUES (?, ?)",
                                    (record["id"], json.dumps(record))
                                )
                    database.commit()
                for row in database.execute("SELECT payload FROM records ORDER BY rowid"):
                    record = json.loads(row[0])
                    if "locationsForMove" not in Context.get_unselected_fields(self.name):
                        record["locationsForMove"] = self.get_locations_for_move(record["id"])
                    yield record

    def get_objects(self):
        last_updated_at = self.get_bookmark()
        sync_start = utils.now().replace(microsecond=0)
        queries = self.get_bulk_queries()
        query_hash = hashlib.sha256(json.dumps(queries).encode("utf-8")).hexdigest()
        self._bulk_query_hash = query_hash
        stored = Context.state.get("bookmarks", {}).get(self.name, {}).get("bulk_operation")
        if stored and (stored.get("query_hash") != query_hash or
                       stored.get("last_date_window") != self.date_window_size or
                       stored.get("window_start") != utils.strftime(last_updated_at)):
            self.clear_bulk_operation_state()
            stored = None
        while last_updated_at < sync_start:
            query_end = (utils.strptime_to_utc(stored["window_end"]) if stored else
                         min(sync_start, last_updated_at + timedelta(days=self.date_window_size)))
            yield from self._get_window_records(last_updated_at, query_end, queries, stored)
            last_updated_at = query_end
            self.update_bookmark(utils.strftime(last_updated_at))
            self.clear_bulk_operation_state()
            stored = None

    def get_locations_for_move(self, fulfillment_order_id):
        """Fetch the non-Node connection separately, with bounded query cost."""
        query = """
            query LocationsForMove($id: ID!, $after: String) {
                fulfillmentOrder(id: $id) {
                    locationsForMove(first: 1, after: $after) {
                        edges { node {
                            message movable location { id }
                            unavailableLineItemsCount { count precision }
                            availableLineItemsCount { count precision }
                            availableLineItems(first: 100) {
                                nodes { id } pageInfo { hasNextPage endCursor }
                            }
                            unavailableLineItems(first: 100) {
                                nodes { id } pageInfo { hasNextPage endCursor }
                            }
                        } }
                        pageInfo { hasNextPage endCursor }
                    }
                }
            }
        """
        params = {"id": fulfillment_order_id}
        locations = []
        while True:
            response = self.call_api(params, query=query, data_key="fulfillmentOrder")
            connection = response["locationsForMove"]
            for edge in connection["edges"]:
                location = edge["node"]
                for key in ("availableLineItems", "unavailableLineItems"):
                    items = location[key]["nodes"]
                    page_info = location[key]["pageInfo"]
                    while page_info["hasNextPage"]:
                        child_query = """
                            query MoveLineItems($id: ID!, $locationIds: [ID!], $after: String) {
                                fulfillmentOrder(id: $id) {
                                    locationsForMove(first: 1, locationIds: $locationIds) {
                                        edges { node { %s(first: 100, after: $after) {
                                            nodes { id } pageInfo { hasNextPage endCursor }
                                        } } }
                                    }
                                }
                            }
                        """ % key
                        child = self.call_api({"id": fulfillment_order_id,
                                               "locationIds": [location["location"]["id"]],
                                               "after": page_info["endCursor"]},
                                              query=child_query, data_key="fulfillmentOrder")
                        page = child["locationsForMove"]["edges"][0]["node"][key]
                        items.extend(page["nodes"])
                        page_info = page["pageInfo"]
                    location[key] = items
                locations.append(location)
            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                return locations
            params["after"] = page_info["endCursor"]

    def transform_object(self, obj):
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
