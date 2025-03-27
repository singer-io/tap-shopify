from tap_shopify.context import Context
from tap_shopify.streams.base import Stream


class Customers(Stream):
    """Stream class for Shopify Customers."""
    name = "customers"
    data_key = "customers"
    replication_key = "updatedAt"

    def get_query(self):
        """
        Returns the GraphQL query for fetching customers.

        Returns:
            str: GraphQL query string.
        """
        return """
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
            }
        """

Context.stream_objects["customers"] = Customers
