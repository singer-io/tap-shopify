import json
import datetime
import urllib
import backoff
import requests
import shopify
import singer
from tap_shopify.streams.base import get_request_timeout
from tap_shopify.exceptions import ShopifyError

LOGGER = singer.get_logger()

# Shopify client credentials tokens are valid for 24 hours
TOKEN_VALIDITY_SECONDS = 86400

# Refresh 30 minutes before actual expiry to avoid extraction interruptions.
TOKEN_EXPIRY_BUFFER_SECONDS = 1800

SHOPIFY_API_VERSION = '2025-07'

class ShopifyClient:
    """
    Handles Shopify authentication via client credentials grant type.

    - Manages access token lifecycle (expiry check, refresh)
    - Saves refreshed tokens back to the config file
    - Supports dev mode (uses existing token, never refreshes)
    """

    def __init__(self, config_path, config, dev_mode=False):
        self.config_path = config_path
        self.config = config
        self.dev_mode = dev_mode
        self.access_token = config['access_token']

        self.refresh_if_expired()


    def _is_token_valid(self):
        """Check if the current access token is still valid (not expired)."""
        token_expires_at = self.config.get('token_expires_at')

        if not token_expires_at:
            LOGGER.info("No 'token_expires_at' in config. Will refresh the access_token.")
            return False

        try:
            expires_at = datetime.datetime.fromisoformat(token_expires_at)
        except (ValueError, TypeError):
            LOGGER.warning("Invalid 'access_token_expires_at': %s. Will refresh.", token_expires_at)
            return True

        now = datetime.datetime.now(datetime.timezone.utc)
        buffer = datetime.timedelta(seconds=TOKEN_EXPIRY_BUFFER_SECONDS)

        if now >= (expires_at - buffer):
            LOGGER.info("Access token expiring soon (expires_at: %s). Refreshing it now.",
                        token_expires_at)
            return False
        return True

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
        response = requests.post(token_url, json=payload, timeout=30)

        if response.status_code != 200:
            raise ShopifyError(
                urllib.error.HTTPError,
                f"Failed to obtain access token. "
                f"Status: {response.status_code}, Response: {response.text}"
            )

        token_data = response.json()
        self.access_token = token_data['access_token']

        # Calculate expiry: use expires_in from response or default to 24 hours
        expires_in = token_data.get('expires_in', TOKEN_VALIDITY_SECONDS)
        token_expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=expires_in)

        # Update config in memory
        self.config['access_token'] = self.access_token
        self.config['token_expires_at'] = token_expires_at.isoformat()

        # Write back to config file
        self._write_config()

    def _write_config(self):
        """Save updated config (with new token) back to config file."""
        LOGGER.info("Saving credentials back to config")

        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        config['access_token'] = self.config['access_token']
        config['token_expires_at'] = self.config['token_expires_at']

        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

    def refresh_if_expired(self):
        """
        Check token expiry and refresh if needed.

        Returns:
            True if token was refreshed, False otherwise.
        """
        if self.dev_mode:
            return False

        if not self._is_token_valid():
            self._refresh_access_token()
            return True

        return False

    def reinitialize_session(self):
        """Reinitialize the Shopify session with the current access token."""
        session = shopify.Session(self.config['shop'], SHOPIFY_API_VERSION, self.access_token)
        shopify.ShopifyResource.activate_session(session)

        # set request timeout
        shopify.Shop.set_timeout(get_request_timeout())
        LOGGER.info("Shopify session reinitialized with refreshed token")
