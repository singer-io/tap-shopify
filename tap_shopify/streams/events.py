from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream, get_events_query



class Events(ShopifyGqlStream):
    name = 'events'
    data_key = "events"
    replication_key = "createdAt"

    # pylint: disable=W0221
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering, pagination
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
        qry = """
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
            }"""
        return qry

Context.stream_objects['events'] = Events
