from abc import ABC, abstractmethod
from datetime import timedelta
import json

from singer import utils, get_logger, metrics
from tap_shopify.streams.base import Stream
from tap_shopify.context import Context

LOGGER = get_logger()


class Metafields(Stream, ABC):
    """Stream class for Shopify Metafields"""

    name = None
    data_key = None
    child_data_key = "metafields"
    replication_key = "updatedAt"

    @abstractmethod
    def get_query(self):
        """Placeholder for get_query method."""

    def transform_object(self, obj):
        """
        Transforms a metafield object for output.
        """
        obj["value_type"] = obj.get("type") or None
        obj["updated_at"] = obj.get("updatedAt")
        if obj["value_type"] in ["json", "weight", "volume", "dimension", "rating"]:
            value = obj.get("value")
            try:
                obj["value"] = json.loads(value) if value is not None else value
            except json.decoder.JSONDecodeError:
                LOGGER.info("Failed to decode JSON value for obj %s", obj.get("id"))
                obj["value"] = value
        return obj

    def fetch_paginated_child_data(self, initial_child_data, parent_id):
        """
        Fetches all pages of child data by handling pagination.
        """
        # Extract the numeric ID from the full path
        numeric_id = parent_id.split('/')[-1]
        page_info = initial_child_data.get("pageInfo", {})

        while page_info.get("hasNextPage"):
            query_params = {
                "first": self.results_per_page,
                "query": f"id:{numeric_id}",
                "childafter": page_info.get("endCursor"),
            }

            response = self.call_api(query_params)
            response_edges = response.get("edges", [])
            if not response_edges:
                break

            first_edge = response_edges[0]
            child_data = first_edge.get("node", {}).get(self.child_data_key, {})

            yield from child_data.get("edges", [])

            page_info = child_data.get("pageInfo", {})

    # pylint: disable=too-many-locals, too-many-nested-blocks
    def get_objects(self):
        """
        Main iterator to yield metafield objects.
        """
        sync_start = utils.now().replace(microsecond=0)
        # Will always fetch the data from the start date and filter it in the code
        # Shopify doesn't support filtering by updated_at for metafields
        last_updated_at = utils.strptime_with_tz(Context.config["start_date"])

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)

            has_next_page = True
            cursor = None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params)

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
                        for child_obj in self.fetch_paginated_child_data(
                            node.get(self.child_data_key), parent_id
                        ):
                            transformed_obj = self.transform_object(child_obj.get("node"))
                            yield transformed_obj

                page_info = data.get("pageInfo", {})
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            last_updated_at = query_end

    def sync(self):
        """
        Performs pseudo incremental sync.
        """
        start_time = utils.now().replace(microsecond=0)
        max_bookmark_value = current_bookmark_value = self.get_bookmark()

        for obj in self.get_objects():
            replication_value = utils.strptime_to_utc(obj[self.replication_key])

            max_bookmark_value = max(max_bookmark_value, replication_value)

            if replication_value >= current_bookmark_value:
                yield obj

        # Update bookmark to the latest value, but not beyond sync start time
        max_bookmark_value = min(start_time, max_bookmark_value)
        self.update_bookmark(utils.strftime(max_bookmark_value))
