from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import DATE_WINDOW_SIZE
from tap_shopify.streams.graphql import ShopifyGqlStream


class InventoryLevels(ShopifyGqlStream):
    name = "inventory_levels"
    data_key = "locations"
    child_data_key = "inventoryLevels"
    replication_key = "updatedAt"

    # pylint: disable=arguments-differ
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination.
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
        """
        Gets all child objects efficiently with pagination.
        """
        query = self.get_child_query(parent_id)
        has_next_page = True
        while has_next_page:
            child_query_params = {
                "after": cursor,
                "parent_id": parent_id,
                "first": self.results_per_page,
            }
            data = self.call_api(child_query_params, query, "location")
            child_data = data.get(self.child_data_key, {})

            # Yield all edges in the current page
            yield from child_data.get("edges", [])

            page_info = child_data.get("pageInfo", {})
            cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage", False)

    # pylint: disable=too-many-locals, too-many-nested-blocks
    def get_objects(self):
        """
        Retrieves objects in paginated batches.
        """
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

                # Process parent objects
                for edge in data.get("edges", []):
                    node = edge.get("node", {})

                    # Handle already fetched child objects
                    child_edges = node.get(self.child_data_key, {}).get("edges", [])
                    for child_obj in child_edges:
                        obj = self.transform_object(child_obj.get("node"))
                        yield obj

                    # Check if more child pages are needed
                    child_page_info = node.get(self.child_data_key, {}).get("pageInfo", {})
                    if child_page_info.get("hasNextPage", False):
                        parent_id = node.get("id")
                        child_cursor = child_page_info.get("endCursor")

                        # Get remaining child pages
                        for child_obj in self.get_next_page_child(parent_id, child_cursor):
                            transformed_obj = self.transform_object(child_obj.get("node"))
                            yield transformed_obj

                page_info = data.get("pageInfo", {})
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            # Move to the next date window
            last_updated_at = query_end

    def sync(self):
        """
        Sync inventory levels and update bookmarks.
        """
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
        """
        Returns the GraphQL query for inventory levels.
        """
        return """query GetInventoryLevels($first: Int!, $after: String, $query: String) {
            locations(first: $first, after: $after, sortKey: ID, includeInactive: true, includeLegacy: true) {
                edges {
                    node {
                        inventoryLevels(first: $first, query: $query) {
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

    @classmethod
    def get_child_query(cls, parent_id):
        """
        Returns the GraphQL query for child inventory levels.
        """
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


Context.stream_objects["inventory_levels"] = InventoryLevels
