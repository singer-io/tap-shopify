from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Webhooks(Stream):
    """Stream class for Webhooks in Shopify."""
    name = "webhooks"
    data_key = "webhookSubscriptions"
    replication_key = "updatedAt"

    def get_query(self):
        return """
        query Webhooks($first: Int!, $after: String, $query: String) {
            webhookSubscriptions(first: $first, after: $after, query: $query) {
                edges {
                    node {
                        id
                        format
                        filter
                        createdAt
                        apiVersion {
                            displayName
                            handle
                            supported
                        }
                        includeFields
                        legacyResourceId
                        metafieldNamespaces
                        topic
                        updatedAt
                        endpoint {
                            ... on WebhookEventBridgeEndpoint {
                                __typename
                                arn
                            }
                            ... on WebhookHttpEndpoint {
                                __typename
                                callbackUrl
                            }
                            ... on WebhookPubSubEndpoint {
                                __typename
                                pubSubProject
                                pubSubTopic
                            }
                        }
                        callbackUrl
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

Context.stream_objects["webhooks"] = Webhooks
