from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class Events(ShopifyGqlStream):
    """Stream class for Shopify Events."""

    name = "events"
    data_key = "events"
    replication_key = "createdAt"

    # pylint: disable=arguments-differ
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query parameters for filtering and pagination.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str, optional): Pagination cursor.

        Returns:
            dict: Query parameters.
        """
        filter_key = "created_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

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
