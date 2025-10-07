from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class MarketingEvents(Stream):
    """Stream class for Marketing Events in Shopify."""
    name = "marketing_events"
    data_key = "marketingEvents"
    replication_key = "started_at"
    access_scope = ["read_marketing_events"]

    def get_query(self):
        return """
        query MarketingEvents($first: Int!, $after: String, $query: String) {
            marketingEvents(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        type
                        remoteId
                        startedAt
                        endedAt
                        scheduledToEndAt
                        manageUrl
                        previewUrl
                        utmCampaign
                        utmMedium
                        utmSource
                        description
                        marketingChannelType
                        sourceAndMedium
                        channelHandle
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["marketing_events"] = MarketingEvents
