import singer
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class ResourceFeedback(Stream):
    """Stream class for Resource Feedback in Shopify."""
    name = "resource_feedback"
    data_key = "app"
    replication_key = "feedback_generated_at"
    key_properties = ["app_id"]
    access_scope = ["read_products"]

    def transform_object(self, obj, **_kwargs):
        """
        If the replication key is missing in the input node, a fallback timestamp (current UTC time)
        is injected to allow the pipeline to continue operating in incremental sync mode.
        Ensures the replication key is returned as an ISO string.

        Args:
            obj (dict): object.
            **_kwargs: Optional additional parameters.
        Returns:
            dict: Transformed product object.
        """
        if "id" in obj:
            obj["app_id"] = obj.pop("id")

        feedback = obj.get("feedback")
        feedback_generated_at = feedback.get("feedbackGeneratedAt") if feedback else None

        if feedback_generated_at:
            obj[self.replication_key] = feedback_generated_at
        elif not obj.get(self.replication_key):
            last_updated_at = _kwargs["last_updated_at"]
            obj[self.replication_key] = last_updated_at.isoformat()
        return obj

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Transformation and Bookmarking
        """
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))
        query_params = {}

        with metrics.http_request_timer(self.name):
            data = self.call_api(query_params, query=query)

        obj = self.transform_object(data, last_updated_at=last_updated_at)
        replication_value = utils.strptime_to_utc(obj[self.replication_key])
        if replication_value >= current_bookmark:
            yield obj

        current_bookmark = max(current_bookmark, replication_value)
        self.update_bookmark(utils.strftime(current_bookmark))

    def get_query(self):
        return """
        query Redirects {
            app {
                id
                title
                feedback {
                    messages {
                        field
                        message
                    }
                    state
                    feedbackGeneratedAt
                    link {
                        label
                        url
                    }
                }
            }
        }
        """

Context.stream_objects["resource_feedback"] = ResourceFeedback
