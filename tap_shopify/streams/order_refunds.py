from singer import(
    metrics,
    utils
)
from tap_shopify.streams.base import DATE_WINDOW_SIZE
from tap_shopify.context import Context

from datetime import timedelta
from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream

class OrderRefunds(ShopifyGqlStream):
    name = 'order_refunds'
    data_key = "orders"
    child_data_key = "refunds"
    replication_key = "updatedAt"

    # pylint: disable=W0221
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

    def get_next_page_child(self, parent_id, cursor):
        """Gets all child objects efficiently with pagination"""
        query = self.get_child_query(parent_id)
        has_next_page = True
        while has_next_page:
            child_query_params = {
                "after": cursor,
                "parent_id": parent_id,
                "first": self.results_per_page
            }
            data = self.call_api(child_query_params, query, "location")
            child_data = data.get(self.child_data_key, {})

            # Yield all edges in current page
            yield from child_data.get("edges", [])

            page_info = child_data.get("pageInfo", {})
            cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage", False)

    # pylint: disable=too-many-locals, too-many-nested-blocks
    def get_objects(self):
        last_updated_at = self.get_bookmark()
        sync_start = utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        # Process each date window
        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=date_window_size)
            query_end = min(sync_start, date_window_end)
            has_next_page, cursor = True, None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)
                    print(data)

                # Process parent objects
                for edge in data.get("edges", []):
                    node = edge.get("node", {})

                    # First handle the already fetched child objects
                    child_edges = node.get(self.child_data_key).get("edges", [])
                    for child_obj in child_edges:
                        obj = self.transform_object(child_obj.get("node"))
                        yield obj

                    # Check if we need to get more child pages
                    child_page_info = node.get(self.child_data_key, {}).get("pageInfo", {})
                    if child_page_info.get("hasNextPage", False):
                        parent_id = node.get("id")
                        child_cursor = child_page_info.get("endCursor")

                        # Get remaining child pages
                        for child_obj in self.get_next_page_child(parent_id, child_cursor):
                            transformed_obj = self.transform_object(child_obj.get("node"))
                            yield transformed_obj

                page_info =  data.get("pageInfo", {})
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            # Move to next date window
            last_updated_at = query_end

    def sync(self):
        """Sync metafields and update bookmarks"""
        start_time = utils.now().replace(microsecond=0)
        max_bookmark_value = current_bookmark_value = self.get_bookmark()

        for obj in self.get_objects():
            replication_value = utils.strptime_to_utc(obj["updatedAt"])

            if replication_value > max_bookmark_value:
                max_bookmark_value = replication_value

            if replication_value >= current_bookmark_value:
                yield obj

        max_bookmark_value = min(start_time, max_bookmark_value)
        self.update_bookmark(utils.strftime(max_bookmark_value))

    def get_query(self):
        qry = """query GetOrderRefunds($first: Int!, $after: String, $query: String) {
                orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                    edges {
                    node {
                        refunds(first: 10) {
                        id
                        refundLineItems(first: 10) {
                            edges {
                            node {
                                id
                                quantity
                                priceSet {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                                }
                                restockType
                                restocked
                                subtotalSet {
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                                }
                                totalTaxSet {
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
                            pageInfo {
                            endCursor
                            hasNextPage
                            }
                        }
                        createdAt
                        legacyResourceId
                        note
                        order {
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
            }"""
        return qry

    def get_child_query(self, parent_id):
        pass

Context.stream_objects['order_refunds'] = OrderRefunds
