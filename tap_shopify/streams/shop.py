import singer
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

LOGGER = singer.get_logger()


class Shop(Stream):
    """Stream class for hop in Shopify."""
    name = "shop"
    data_key = "shop"

    # pylint: disable=too-many-locals
    def get_objects(self):
        """
        Returns:
            - Yields list of objects for the stream
        Performs:
            - Transformation
        """
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        query_params = {}

        with metrics.http_request_timer(self.name):
            data = self.call_api(query_params, query=query)
            obj = self.transform_object(data)
            yield obj

    def get_query(self):
        return """
        query ShopShow {
            shop {
                alerts {
                    action {
                        title
                        url
                    }
                    description
                }
                billingAddress {
                    address1
                    address2
                    city
                    company
                    country
                    countryCodeV2
                    latitude
                    longitude
                    phone
                    province
                    provinceCode
                    zip
                }
                checkoutApiSupported
                contactEmail
                createdAt
                currencyCode
                currencyFormats {
                    moneyFormat
                    moneyInEmailsFormat
                    moneyWithCurrencyFormat
                    moneyWithCurrencyInEmailsFormat
                }
                customerAccounts
                description
                email
                enabledPresentmentCurrencies
                fulfillmentServices {
                    handle
                    serviceName
                }
                ianaTimezone
                id
                marketingSmsConsentEnabledAtCheckout
                myshopifyDomain
                name
                paymentSettings {
                    supportedDigitalWallets
                }
                plan {
                    partnerDevelopment
                    shopifyPlus
                    publicDisplayName
                }
                primaryDomain {
                    host
                    id
                }
                setupRequired
                shipsToCountries
                taxesIncluded
                taxShipping
                timezoneAbbreviation
                transactionalSmsDisabled
                updatedAt
                url
                weightUnit
            }
        }
        """

Context.stream_objects["shop"] = Shop
