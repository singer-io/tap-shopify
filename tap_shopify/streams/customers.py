from tap_shopify.context import Context
from tap_shopify.streams.graphql import get_customers_query
from tap_shopify.streams.graphql import ShopifyGqlStream



class Customers(ShopifyGqlStream):
    name = 'customers'
    data_key = "customers"
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

    def get_query(self):
        qry = """
                query Customers($first: Int!, $after: String, $query: String) {
                    customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                        edges {
                        node {
                            email
                            multipassIdentifier
                            defaultAddress {
                            city
                            address1
                            zip
                            id
                            province
                            phone
                            country
                            firstName
                            lastName
                            countryCodeV2
                            name
                            provinceCode
                            address2
                            company
                            timeZone
                            validationResultSummary
                            latitude
                            longitude
                            coordinatesValidated
                            formattedArea
                            }
                            numberOfOrders
                            state
                            verifiedEmail
                            firstName
                            updatedAt
                            note
                            phone
                            addresses(first: 250) {
                            city
                            address1
                            zip
                            id
                            province
                            phone
                            country
                            firstName
                            lastName
                            countryCodeV2
                            name
                            provinceCode
                            address2
                            company
                            timeZone
                            validationResultSummary
                            latitude
                            longitude
                            coordinatesValidated
                            formattedArea
                            }
                            lastName
                            tags
                            taxExempt
                            id
                            createdAt
                            taxExemptions
                            emailMarketingConsent {
                            consentUpdatedAt
                            marketingOptInLevel
                            marketingState
                            }
                            smsMarketingConsent {
                            consentCollectedFrom
                            consentUpdatedAt
                            marketingOptInLevel
                            marketingState
                            }
                            validEmailAddress
                            productSubscriberStatus
                            amountSpent {
                            amount
                            currencyCode
                            }
                            dataSaleOptOut
                            displayName
                            locale
                            lifetimeDuration
                        }
                        }
                        pageInfo {
                        endCursor
                        hasNextPage
                        }
                    }
                    }"""
        return qry

Context.stream_objects['customers'] = Customers
