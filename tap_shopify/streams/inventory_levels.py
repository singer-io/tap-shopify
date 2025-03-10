from singer import(
    metrics,
    get_logger,
    utils
)
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      DATE_WINDOW_SIZE,
                                      shopify_error_handling)
from tap_shopify.context import Context

from datetime import timedelta
from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream

class InventoryLevels(ShopifyGqlStream):
    name = 'inventory_levels'
    data_key = "locations"
    child_data_key = "inventoryLevels"
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

    def get_next_page_child(self, edge):
        parent_id = edge.get("node").get("id")
        query = self.get_child_query(parent_id)
        has_next_page = True
        child_query_params = {}
        cursor = edge.get("node", {}).get(self.child_data_key, {}).get("pageInfo", {}).get("endCursor")
        while has_next_page:
            child_query_params["after"] = cursor
            child_query_params["parent_id"] = parent_id
            child_query_params["first"] = self.results_per_page
            data = self.call_api(child_query_params, query, "location")
            child_data = data.get(self.child_data_key, {})
            for edge in child_data.get("edges", []):
                yield edge
            page_info = data.get("pageInfo", {})
            cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage", False)

    # pylint: disable=too-many-locals, too-many-nested-blocks
    def get_objects(self):
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        sync_start = utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=date_window_size)
            query_end = min(sync_start, date_window_end)
            has_next_page, cursor = True, None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

                for edge in data.get("edges", []):
                    child_objects = edge.get("node").get(self.child_data_key).get("edges", [])
                    for child_obj in child_objects:
                        obj = self.transform_object(child_obj.get("node"))
                        replication_value = utils.strptime_to_utc(obj[self.replication_key])
                        if replication_value > current_bookmark:
                            current_bookmark = replication_value
                        yield obj
                    child_page_info = edge.get("node", {}).get(self.child_data_key, {}).get("pageInfo", {})
                    child_has_next_page = child_page_info.get("hasNextPage", False)
                    if child_has_next_page:
                        for child_obj in self.get_next_page_child(edge):
                            yield self.transform_object(child_obj.get("node"))

                page_info =  data.get("pageInfo", {})
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

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
        qry = """query GetInventoryLevels($first: Int!, $after: String, $query: String) {
                locations(first: $first, after: $after, sortKey: ID) {
                    edges {
                    node {
                        inventoryLevels(first: 10, query: $query) {
                        edges {
                            node {
                            canDeactivate
                            createdAt
                            deactivationAlert
                            id
                            location {
                                id
                            }
                            updatedAt
                            item {
                                id
                                variant {
                                id
                                }
                            }
                            quantities(names: ["available", "committed", "damaged", "incoming", "on_hand", "quality_control", "reserved", "safety_stock"]) {
                            id
                            name
                            quantity
                            updatedAt
                        }
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        }
                        id
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
        return f"""
            query GetInventoryLevels($first: Int!, $after: String, $query: String) {{
            location(id: "{parent_id}") {{
                inventoryLevels(first: $first, after: $after, query: $query) {{
                edges {{
                    node {{
                    canDeactivate
                    createdAt
                    deactivationAlert
                    id
                    updatedAt
                    item {{
                        id
                        variant {{
                        id
                        }}
                    }}
                    location {{
                        id
                    }}
                    quantities(
                        names: ["available", "committed", "damaged", "incoming", "on_hand", "quality_control", "reserved", "safety_stock"]
                    ) {{
                        id
                        name
                        quantity
                        updatedAt
                    }}
                    }}
                }}
                pageInfo {{
                    endCursor
                    hasNextPage
                }}
                }}
            }}
            }}
            """

Context.stream_objects['inventory_levels'] = InventoryLevels
