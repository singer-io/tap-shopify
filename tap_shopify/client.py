import json
import urllib.error
import backoff
import requests
import shopify
import singer
from tap_shopify.streams.base import get_request_timeout
from tap_shopify.exceptions import ShopifyError

LOGGER = singer.get_logger()

SHOPIFY_API_VERSION = '2025-07'

class ShopifyClient:
    """
    Handles Shopify authentication via client credentials grant type.

    - Fetches an access token on startup if one is not already present
    - Re-fetches the token when the API returns a 401 (token expired or revoked)
    - Saves refreshed tokens back to the config file
    """

    def __init__(self, config_path, config):
        self.config_path = config_path
        self.config = config
        self.access_token = config.get('access_token')

        if not self.access_token:
            self._refresh_access_token()


    # pylint: disable=broad-exception-caught
    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=3,
                          factor=2)
    def _refresh_access_token(self):
        """Generate a new access token using client credentials grant type."""
        shop = self.config['shop']
        client_id = self.config['client_id']
        client_secret = self.config['client_secret']

        token_url = f"https://{shop}.myshopify.com/admin/oauth/access_token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }

        LOGGER.info("Requesting new access token via client credentials grant")
        response = requests.post(token_url, json=payload,
                                 headers={"Accept": "application/json"},
                                 timeout=30)

        if response.status_code != 200:
            try:
                error_data = response.json()
                error_detail = (
                    error_data.get('error_description')
                    or error_data.get('error')
                    or response.text
                )
            except Exception:
                error_detail = response.text

            raise ShopifyError(
                urllib.error.HTTPError(
                    url=token_url,
                    code=response.status_code,
                    msg=error_detail,
                    hdrs={},
                    fp=None
                ),
                f"Failed to obtain access token. "
                f"Status: {response.status_code}. {error_detail}"
            )

        token_data = response.json()
        self.access_token = token_data['access_token']

        # Update config in memory
        self.config['access_token'] = self.access_token

        # Write back to config file
        self._write_config()

    def _write_config(self):
        """Save updated config (with new token) back to config file."""
        LOGGER.info("Saving credentials back to config")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        config['access_token'] = self.config['access_token']

        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

    def refresh_token(self):
        """Force a token refresh. Called when the API returns a 401 (token expired or revoked)."""
        self._refresh_access_token()

    def reinitialize_session(self):
        """Reinitialize the Shopify session with the current access token."""
        session = shopify.Session(self.config['shop'], SHOPIFY_API_VERSION, self.access_token)
        shopify.ShopifyResource.activate_session(session)

        # set request timeout
        shopify.Shop.set_timeout(get_request_timeout())
        LOGGER.info("Shopify session reinitialized with refreshed token")
