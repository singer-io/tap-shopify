from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class InventoryLevels(Stream):
    name = "inventory_levels"
    data_key = "locations"
    child_data_key = "inventoryLevels"
    replication_key = "updatedAt"

    def get_next_page_child(self, parent_id, cursor, child_query):
        """
        Gets all child objects efficiently with pagination.
        """
        has_next_page = True
        while has_next_page:
            child_query_params = {
                "first": self.results_per_page,
                "parentquery": f"id:{parent_id.split('/')[-1]}",
                "query": child_query,
                "childafter": cursor,
            }
            data = self.call_api(child_query_params)
            for edge in data.get("edges", []):
                node = edge.get("node", {})

                child_data = node.get(self.child_data_key, {})
                child_edges = child_data.get("edges", [])
                for child_obj in child_edges:
                    yield child_obj

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

        # Process each date window
        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
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
                        for child_obj in self.get_next_page_child(parent_id, child_cursor, query_params["query"]):
                            transformed_obj = self.transform_object(child_obj.get("node"))
                            yield transformed_obj

                page_info = data.get("pageInfo", {})
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            # Move to the next date window
            last_updated_at = query_end

    def get_query(self):
        """
        Returns the GraphQL query for inventory levels.
        """
        return """query GetInventoryLevels($first: Int!, $after: String, $query: String, $childafter: String, $parentquery: String) {
            locations(first: $first, after: $after, query: $parentquery, sortKey: ID, includeInactive: true, includeLegacy: true) {
                edges {
                    node {
                        inventoryLevels(first: $first, query: $query, after: $childafter) {
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

Context.stream_objects["inventory_levels"] = InventoryLevels
