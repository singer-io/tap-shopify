from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Events(Stream):
    """Stream class for Shopify Events."""

    name = "events"
    data_key = "events"
    replication_key = "createdAt"

    def get_query(self):
        """
        Returns the GraphQL query for fetching events.

        Returns:
            str: GraphQL query string.
        """
        return """
            query GetEvents($first: Int!, $after: String, $query: String) {
                events(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
                    edges {
                        node {
                            id
                            createdAt
                            action
                            appTitle
                            attributeToApp
                            attributeToUser
                            criticalAlert
                            message
                            ... on BasicEvent {
                                id
                                subjectId
                                subjectType
                                action
                                additionalContent
                                additionalData
                                appTitle
                                arguments
                                attributeToApp
                                attributeToUser
                                createdAt
                                criticalAlert
                                hasAdditionalContent
                                message
                                secondaryMessage
                            }
                            ... on CommentEvent {
                                id
                                action
                                appTitle
                                attachments {
                                    fileExtension
                                    id
                                    name
                                    size
                                    url
                                }
                                attributeToApp
                                attributeToUser
                                author {
                                    id
                                }
                                canDelete
                                canEdit
                                createdAt
                                criticalAlert
                                edited
                                message
                                rawMessage
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


Context.stream_objects["events"] = Events
