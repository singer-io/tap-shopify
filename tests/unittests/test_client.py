import json
import os
import datetime
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import requests

from tap_shopify.client import (
    ShopifyClient,
    TOKEN_VALIDITY_SECONDS,
    TOKEN_EXPIRY_BUFFER_SECONDS,
    SHOPIFY_API_VERSION,
)
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError

def _iso_future(seconds=7200):
    """Helper to generate an ISO timestamp in the future."""
    return (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=seconds)).isoformat()


def _iso_past(seconds=100):
    """Helper to generate an ISO timestamp in the past."""
    return (datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=seconds)).isoformat()


class TestShopifyClientInit(unittest.TestCase):
    """Tests for ShopifyClient initialization."""

    def _make_config(self, **overrides):
        base = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "existing_token",
            "token_expires_at": _iso_future(7200),
            "start_date": "2025-01-01T00:00:00Z",
        }
        base.update(overrides)
        return base

    def _write_config_file(self, config):
        """Write config dict to a temp file and return its path."""
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()
        return tmp.name

    # -------------------------------------------------------------------------
    # Dev mode tests
    # -------------------------------------------------------------------------
    def test_dev_mode_with_access_token(self):
        """Dev mode should use existing access_token and not refresh."""
        config = self._make_config()
        path = self._write_config_file(config)
        try:
            client = ShopifyClient(path, config, dev_mode=True)
            self.assertTrue(client.dev_mode)
            self.assertEqual(client.access_token, "existing_token")
        finally:
            os.unlink(path)

    def test_dev_mode_without_access_token_raises(self):
        """Dev mode without access_token should raise KeyError."""
        config = self._make_config()
        del config['access_token']
        path = self._write_config_file(config)
        try:
            with self.assertRaises(KeyError):
                ShopifyClient(path, config, dev_mode=True)
        finally:
            os.unlink(path)

    # -------------------------------------------------------------------------
    # Non-dev mode — valid token
    # -------------------------------------------------------------------------
    def test_init_with_valid_token(self):
        """If token is not expired, no refresh should occur."""
        config = self._make_config(token_expires_at=_iso_future(7200))
        path = self._write_config_file(config)
        try:
            with patch.object(ShopifyClient, '_refresh_access_token') as mock_refresh:
                client = ShopifyClient(path, config, dev_mode=False)
                mock_refresh.assert_not_called()
                self.assertEqual(client.access_token, "existing_token")
        finally:
            os.unlink(path)

    # -------------------------------------------------------------------------
    # Non-dev mode — expired token triggers refresh
    # -------------------------------------------------------------------------
    @patch('tap_shopify.client.requests.post')
    def test_init_with_expired_token_triggers_refresh(self, mock_post):
        """Expired token should trigger a token refresh."""
        config = self._make_config(token_expires_at=_iso_past(100))
        path = self._write_config_file(config)
        try:
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "access_token": "new_token",
                    "expires_in": TOKEN_VALIDITY_SECONDS,
                }),
            )
            client = ShopifyClient(path, config, dev_mode=False)
            mock_post.assert_called_once()
            self.assertEqual(client.access_token, "new_token")
            self.assertEqual(config['access_token'], "new_token")
        finally:
            os.unlink(path)

    @patch('tap_shopify.client.requests.post')
    def test_init_calls_refresh_if_expired(self, mock_post):
        """__init__ should call refresh_if_expired on construction."""
        config = self._make_config(token_expires_at=_iso_past(100))
        path = self._write_config_file(config)
        try:
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "access_token": "refreshed",
                    "expires_in": TOKEN_VALIDITY_SECONDS,
                }),
            )
            with patch.object(ShopifyClient, 'refresh_if_expired', return_value=True) as mock_rfe:
                client = ShopifyClient(path, config, dev_mode=False)
                mock_rfe.assert_called_once()
        finally:
            os.unlink(path)


class TestIsTokenValid(unittest.TestCase):
    """Tests for ShopifyClient._is_token_valid."""

    def _create_client_skip_init(self, config):
        """Create a client instance bypassing __init__ to test individual methods."""
        client = object.__new__(ShopifyClient)
        client.config = config
        client.dev_mode = False
        client.config_path = "/tmp/dummy.json"
        return client

    def test_valid_token(self):
        """Token expiring well in the future should be valid."""
        config = {
            "access_token": "tok",
            "token_expires_at": _iso_future(7200),
        }
        client = self._create_client_skip_init(config)
        self.assertTrue(client._is_token_valid())

    def test_expired_token(self):
        """Token that expired in the past should be invalid."""
        config = {
            "access_token": "tok",
            "token_expires_at": _iso_past(100),
        }
        client = self._create_client_skip_init(config)
        self.assertFalse(client._is_token_valid())

    def test_token_within_buffer_is_invalid(self):
        """Token expiring within the buffer window should be treated as invalid."""
        config = {
            "access_token": "tok",
            "token_expires_at": _iso_future(TOKEN_EXPIRY_BUFFER_SECONDS - 10),
        }
        client = self._create_client_skip_init(config)
        self.assertFalse(client._is_token_valid())

    def test_token_outside_buffer_is_valid(self):
        """Token expiring well beyond the buffer should be valid."""
        config = {
            "access_token": "tok",
            "token_expires_at": _iso_future(TOKEN_EXPIRY_BUFFER_SECONDS + 100),
        }
        client = self._create_client_skip_init(config)
        self.assertTrue(client._is_token_valid())

    def test_missing_token_expires_at_returns_false(self):
        """Missing token_expires_at should return False (triggers refresh)."""
        config = {"access_token": "tok"}
        client = self._create_client_skip_init(config)
        self.assertFalse(client._is_token_valid())

    def test_invalid_format_returns_true(self):
        """Invalid token_expires_at format should return True (logs warning, no refresh)."""
        config = {
            "access_token": "tok",
            "token_expires_at": "not-a-valid-date",
        }
        client = self._create_client_skip_init(config)
        self.assertTrue(client._is_token_valid())

    def test_none_token_expires_at_returns_false(self):
        """Explicit None token_expires_at should return False."""
        config = {"access_token": "tok", "token_expires_at": None}
        client = self._create_client_skip_init(config)
        self.assertFalse(client._is_token_valid())


class TestRefreshAccessToken(unittest.TestCase):
    """Tests for ShopifyClient._refresh_access_token."""

    def _create_client_skip_init(self, config, config_path="/tmp/dummy.json"):
        client = object.__new__(ShopifyClient)
        client.config = config
        client.config_path = config_path
        client.dev_mode = False
        return client

    @patch('tap_shopify.client.requests.post')
    def test_successful_refresh(self, mock_post):
        """Successful token refresh should update config and access_token."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = self._create_client_skip_init(config, tmp.name)
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "access_token": "refreshed_token",
                    "expires_in": 86400,
                }),
            )
            client._refresh_access_token()

            self.assertEqual(client.access_token, "refreshed_token")
            self.assertEqual(config['access_token'], "refreshed_token")
            self.assertIn('token_expires_at', config)
            # token_expires_at is now an ISO string; verify it parses to a future datetime
            expires_at = datetime.datetime.fromisoformat(config['token_expires_at'])
            self.assertGreater(expires_at, datetime.datetime.now(datetime.timezone.utc))

            # Verify the correct endpoint was called (shop + .myshopify.com)
            mock_post.assert_called_once_with(
                "https://test-shop.myshopify.com/admin/oauth/access_token",
                json={
                    "client_id": "cid",
                    "client_secret": "csecret",
                    "grant_type": "client_credentials",
                },
                timeout=30,
            )

            # Verify config file was updated
            with open(tmp.name, 'r') as f:
                saved = json.load(f)
            self.assertEqual(saved['access_token'], "refreshed_token")
            self.assertIn('token_expires_at', saved)
        finally:
            os.unlink(tmp.name)

    @patch('tap_shopify.client.requests.post')
    def test_refresh_uses_default_expiry_when_missing(self, mock_post):
        """When expires_in is missing from response, use default 24h."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = self._create_client_skip_init(config, tmp.name)
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "access_token": "new_tok",
                    # no expires_in
                }),
            )
            before = datetime.datetime.now(datetime.timezone.utc)
            client._refresh_access_token()
            after = datetime.datetime.now(datetime.timezone.utc)

            expires_at = datetime.datetime.fromisoformat(config['token_expires_at'])
            expected_min = before + datetime.timedelta(seconds=TOKEN_VALIDITY_SECONDS)
            expected_max = after + datetime.timedelta(seconds=TOKEN_VALIDITY_SECONDS)
            self.assertGreaterEqual(expires_at, expected_min)
            self.assertLessEqual(expires_at, expected_max)
        finally:
            os.unlink(tmp.name)

    @patch('tap_shopify.client.requests.post')
    def test_refresh_failure_raises(self, mock_post):
        """Non-200 response from token endpoint should raise."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = self._create_client_skip_init(config, tmp.name)
            mock_post.return_value = MagicMock(
                status_code=401,
                text="Unauthorized",
            )
            with self.assertRaises(ShopifyError) as ctx:
                client._refresh_access_token()
            self.assertIn("Failed to obtain access token", str(ctx.exception))
        finally:
            os.unlink(tmp.name)

    @patch('tap_shopify.client.requests.post')
    def test_refresh_stores_iso_format_expiry(self, mock_post):
        """token_expires_at should be stored as an ISO 8601 string."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = self._create_client_skip_init(config, tmp.name)
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={
                    "access_token": "tok",
                    "expires_in": 3600,
                }),
            )
            client._refresh_access_token()

            # Verify it's a string, not a float
            self.assertIsInstance(config['token_expires_at'], str)
            # Verify it's valid ISO format
            parsed = datetime.datetime.fromisoformat(config['token_expires_at'])
            self.assertIsNotNone(parsed.tzinfo)
        finally:
            os.unlink(tmp.name)


class TestWriteConfig(unittest.TestCase):
    """Tests for ShopifyClient._write_config."""

    def test_write_config_updates_file(self):
        """Config file should be updated with new access_token and token_expires_at."""
        original = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "start_date": "2025-01-01T00:00:00Z",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(original, tmp)
        tmp.close()

        expires_iso = _iso_future(86400)
        try:
            config = {
                **original,
                "access_token": "new_token",
                "token_expires_at": expires_iso,
            }
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name
            client.dev_mode = False

            client._write_config()

            with open(tmp.name, 'r') as f:
                saved = json.load(f)

            self.assertEqual(saved['access_token'], "new_token")
            self.assertEqual(saved['token_expires_at'], expires_iso)
            # Original keys preserved
            self.assertEqual(saved['shop'], "test-shop")
            self.assertEqual(saved['client_id'], "cid")
        finally:
            os.unlink(tmp.name)

    def test_write_config_preserves_extra_keys(self):
        """Extra keys in the config file should not be removed."""
        original = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "custom_setting": "keep_me",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(original, tmp)
        tmp.close()

        try:
            config = {
                **original,
                "access_token": "tok",
                "token_expires_at": _iso_future(86400),
            }
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name
            client.dev_mode = False

            client._write_config()

            with open(tmp.name, 'r') as f:
                saved = json.load(f)
            self.assertEqual(saved['custom_setting'], "keep_me")
        finally:
            os.unlink(tmp.name)


class TestRefreshIfExpired(unittest.TestCase):
    """Tests for ShopifyClient.refresh_if_expired."""

    def _create_client_skip_init(self, config, dev_mode=False):
        client = object.__new__(ShopifyClient)
        client.config = config
        client.config_path = "/tmp/dummy.json"
        client.dev_mode = dev_mode
        return client

    def test_dev_mode_returns_false(self):
        """Dev mode should never refresh."""
        client = self._create_client_skip_init(
            {"access_token": "tok", "token_expires_at": _iso_past(100)},
            dev_mode=True,
        )
        result = client.refresh_if_expired()
        self.assertFalse(result)

    @patch.object(ShopifyClient, '_refresh_access_token')
    def test_expired_token_triggers_refresh(self, mock_refresh):
        """Expired token should trigger refresh and return True."""
        client = self._create_client_skip_init({
            "access_token": "tok",
            "token_expires_at": _iso_past(100),
        })
        result = client.refresh_if_expired()
        self.assertTrue(result)
        mock_refresh.assert_called_once()

    @patch.object(ShopifyClient, '_refresh_access_token')
    def test_valid_token_does_not_refresh(self, mock_refresh):
        """Valid token should not trigger refresh and return False."""
        client = self._create_client_skip_init({
            "access_token": "tok",
            "token_expires_at": _iso_future(7200),
        })
        result = client.refresh_if_expired()
        self.assertFalse(result)
        mock_refresh.assert_not_called()

    @patch.object(ShopifyClient, '_refresh_access_token')
    def test_token_in_buffer_triggers_refresh(self, mock_refresh):
        """Token within the expiry buffer should trigger refresh."""
        client = self._create_client_skip_init({
            "access_token": "tok",
            "token_expires_at": _iso_future(TOKEN_EXPIRY_BUFFER_SECONDS - 10),
        })
        result = client.refresh_if_expired()
        self.assertTrue(result)
        mock_refresh.assert_called_once()

    @patch.object(ShopifyClient, '_refresh_access_token')
    def test_missing_token_expires_at_triggers_refresh(self, mock_refresh):
        """Missing token_expires_at should trigger refresh."""
        client = self._create_client_skip_init({
            "access_token": "tok",
        })
        result = client.refresh_if_expired()
        self.assertTrue(result)
        mock_refresh.assert_called_once()


class TestReinitializeSession(unittest.TestCase):
    """Tests for ShopifyClient.reinitialize_session."""

    @patch('tap_shopify.client.shopify.Shop.set_timeout')
    @patch('tap_shopify.client.shopify.ShopifyResource.activate_session')
    @patch('tap_shopify.client.shopify.Session')
    def test_reinitialize_session(self, mock_session_cls, mock_activate, mock_set_timeout):
        """reinitialize_session should create a new session, activate it, and set timeout."""
        client = object.__new__(ShopifyClient)
        client.config = {"shop": "test-shop"}
        client.access_token = "my_token"
        client.dev_mode = False
        client.config_path = "/tmp/dummy.json"

        mock_session_instance = MagicMock()
        mock_session_cls.return_value = mock_session_instance

        client.reinitialize_session()

        mock_session_cls.assert_called_once_with(
            "test-shop",
            SHOPIFY_API_VERSION,
            "my_token",
        )
        mock_activate.assert_called_once_with(mock_session_instance)
        mock_set_timeout.assert_called_once()


class TestRetry401Handler(unittest.TestCase):
    """Tests for the retry_401_handler function used in backoff decorator."""

    def setUp(self):
        self.original_client = Context.client

    def tearDown(self):
        Context.client = self.original_client

    def test_retry_401_handler_refreshes_and_reinitializes(self):
        """retry_401_handler should call refresh_if_expired and reinitialize_session when refresh occurs."""
        from tap_shopify.streams.base import retry_401_handler

        mock_client = MagicMock()
        mock_client.refresh_if_expired.return_value = True
        Context.client = mock_client

        retry_401_handler({'wait': 1, 'tries': 1})

        mock_client.refresh_if_expired.assert_called_once()
        mock_client.reinitialize_session.assert_called_once()

    def test_retry_401_handler_no_reinitialize_when_not_refreshed(self):
        """retry_401_handler should not reinitialize if refresh_if_expired returns False."""
        from tap_shopify.streams.base import retry_401_handler

        mock_client = MagicMock()
        mock_client.refresh_if_expired.return_value = False
        Context.client = mock_client

        retry_401_handler({'wait': 1, 'tries': 1})

        mock_client.refresh_if_expired.assert_called_once()
        mock_client.reinitialize_session.assert_not_called()

    def test_retry_401_handler_dev_mode_does_not_refresh(self):
        """In dev mode, refresh_if_expired returns False so no reinitialize should happen."""
        from tap_shopify.streams.base import retry_401_handler

        mock_client = MagicMock()
        # In dev mode, refresh_if_expired returns False
        mock_client.refresh_if_expired.return_value = False
        Context.client = mock_client

        retry_401_handler({'wait': 1, 'tries': 1})

        mock_client.refresh_if_expired.assert_called_once()
        mock_client.reinitialize_session.assert_not_called()


class TestCallApiWithTokenRefresh(unittest.TestCase):
    """Tests for call_api behaviour regarding token handling."""

    def setUp(self):
        self.original_config = Context.config
        self.original_client = Context.client
        Context.config = {
            "start_date": "2025-01-01T00:00:00Z",
            "date_window_size": 30,
        }
        Context.catalog = {
            "streams": [{
                "tap_stream_id": "products",
                "schema": {"properties": {"id": {"type": "string"}, "updatedAt": {"type": "string"}}},
                "metadata": [],
            }],
        }

    def tearDown(self):
        Context.config = self.original_config
        Context.client = self.original_client

    def _make_stream(self):
        from tap_shopify.streams.base import Stream
        stream = Stream()
        stream.name = "products"
        stream.data_key = "products"
        return stream

    @patch('shopify.GraphQL')
    @patch('tap_shopify.streams.base.Stream.get_query', return_value='{ products { edges { node { id } } } }')
    def test_call_api_success(self, mock_get_query, mock_graphql):
        """call_api should return data on a successful GraphQL response."""
        Context.client = None

        mock_response = {
            "data": {"products": {"edges": [], "pageInfo": {"endCursor": None, "hasNextPage": False}}}
        }
        mock_graphql.return_value.execute.return_value = json.dumps(mock_response)

        stream = self._make_stream()
        result = stream.call_api({"query": "test", "first": 10})
        self.assertEqual(result, mock_response["data"]["products"])

    @patch('shopify.GraphQL')
    @patch('tap_shopify.streams.base.Stream.get_query', return_value='{ products { edges { node { id } } } }')
    def test_call_api_without_client(self, mock_get_query, mock_graphql):
        """call_api should work even if Context.client is None (backward compat)."""
        Context.client = None

        mock_response = {
            "data": {"products": {"edges": [], "pageInfo": {"endCursor": None, "hasNextPage": False}}}
        }
        mock_graphql.return_value.execute.return_value = json.dumps(mock_response)

        stream = self._make_stream()
        result = stream.call_api({"query": "test", "first": 10})
        self.assertEqual(result, mock_response["data"]["products"])

    @patch('shopify.GraphQL')
    @patch('tap_shopify.streams.base.Stream.get_query', return_value='{ products { edges { node { id } } } }')
    def test_non_401_http_error_raises_shopify_error(self, mock_get_query, mock_graphql):
        """Non-401 HTTPError should raise ShopifyError."""
        import urllib.error
        from tap_shopify.exceptions import ShopifyError

        Context.client = None

        http_error = urllib.error.HTTPError(
            url="https://test-shop.myshopify.com/admin/api/graphql.json",
            code=500,
            msg="Internal Server Error",
            hdrs=MagicMock(**{"get.return_value": "req-456"}),
            fp=None,
        )
        mock_graphql.return_value.execute.side_effect = http_error

        stream = self._make_stream()

        with self.assertRaises(ShopifyError):
            stream.call_api({"query": "test", "first": 10})

    @patch('shopify.GraphQL')
    @patch('tap_shopify.streams.base.Stream.get_query', return_value='{ products { edges { node { id } } } }')
    def test_401_http_error_raises_shopify_unauthorized_error(self, mock_get_query, mock_graphql):
        """401 HTTPError should raise ShopifyUnauthorizedError (not ShopifyError)."""
        import urllib.error
        from tap_shopify.exceptions import ShopifyUnauthorizedError

        # Must provide a mock client so the retry_401_handler doesn't blow up
        mock_client = MagicMock()
        mock_client.refresh_if_expired.return_value = False
        Context.client = mock_client

        http_error = urllib.error.HTTPError(
            url="https://test-shop.myshopify.com/admin/api/graphql.json",
            code=401,
            msg="Unauthorized",
            hdrs=MagicMock(**{"get.return_value": "req-789"}),
            fp=None,
        )
        mock_graphql.return_value.execute.side_effect = http_error

        stream = self._make_stream()

        with self.assertRaises(ShopifyUnauthorizedError):
            stream.call_api({"query": "test", "first": 10})


class TestMainDevMode(unittest.TestCase):
    """Tests for dev mode flag in main()."""

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_passes_dev_mode_to_client(self, mock_parse_args, mock_discover, mock_client_cls):
        """main() should pass dev mode flag to ShopifyClient."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "tok",
            "token_expires_at": _iso_future(7200),
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
        mock_args.dev = True
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        mock_parse_args.return_value = mock_args

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "tok"
        mock_client_cls.return_value = mock_client_instance
        mock_discover.return_value = {"streams": []}

        try:
            main()
        except SystemExit:
            pass

        mock_client_cls.assert_called_once_with(
            config_path="/tmp/config.json",
            config=mock_args.config,
            dev_mode=True,
        )

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_no_dev_mode(self, mock_parse_args, mock_discover, mock_client_cls):
        """main() should pass dev_mode=False when --dev is not set."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "tok",
            "token_expires_at": _iso_future(7200),
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
        mock_args.dev = False
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        mock_parse_args.return_value = mock_args

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "tok"
        mock_client_cls.return_value = mock_client_instance
        mock_discover.return_value = {"streams": []}

        try:
            main()
        except SystemExit:
            pass

        mock_client_cls.assert_called_once_with(
            config_path="/tmp/config.json",
            config=mock_args.config,
            dev_mode=False,
        )

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_stores_client_in_context(self, mock_parse_args, mock_discover, mock_client_cls):
        """main() should store the ShopifyClient instance in Context.client."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "tok",
            "token_expires_at": _iso_future(7200),
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
        mock_args.dev = False
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        mock_parse_args.return_value = mock_args

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "tok"
        mock_client_cls.return_value = mock_client_instance
        mock_discover.return_value = {"streams": []}

        try:
            main()
        except SystemExit:
            pass

        self.assertEqual(Context.client, mock_client_instance)

    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_no_client_when_no_access_token(self, mock_parse_args, mock_discover):
        """main() should NOT create ShopifyClient when access_token is not in config."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "api_key": "legacy_key",
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
        mock_args.dev = False
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        mock_parse_args.return_value = mock_args

        mock_discover.return_value = {"streams": []}

        original_client = Context.client
        Context.client = None
        try:
            main()
        except SystemExit:
            pass

        self.assertIsNone(Context.client)
        Context.client = original_client

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_updates_config_access_token(self, mock_parse_args, mock_discover, mock_client_cls):
        """main() should update Context.config['access_token'] from client.access_token."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "old_tok",
            "token_expires_at": _iso_future(7200),
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
        mock_args.dev = False
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        mock_parse_args.return_value = mock_args

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "refreshed_tok"
        mock_client_cls.return_value = mock_client_instance
        mock_discover.return_value = {"streams": []}

        try:
            main()
        except SystemExit:
            pass

        self.assertEqual(Context.config.get('access_token'), "refreshed_tok")


class TestBackoffOnRefresh(unittest.TestCase):
    """Tests for backoff/retry on token refresh failures."""

    @patch('tap_shopify.client.requests.post')
    def test_refresh_retries_on_connection_error(self, mock_post):
        """_refresh_access_token should retry on RequestException."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name
            client.dev_mode = False

            # Fail twice then succeed
            mock_post.side_effect = [
                requests.exceptions.ConnectionError("Connection refused"),
                requests.exceptions.ConnectionError("Connection refused"),
                MagicMock(
                    status_code=200,
                    json=MagicMock(return_value={
                        "access_token": "recovered_token",
                        "expires_in": 86400,
                    }),
                ),
            ]
            client._refresh_access_token()
            self.assertEqual(client.access_token, "recovered_token")
            self.assertEqual(mock_post.call_count, 3)
        finally:
            os.unlink(tmp.name)

    @patch('tap_shopify.client.requests.post')
    def test_refresh_gives_up_after_max_retries(self, mock_post):
        """_refresh_access_token should give up after max retries."""
        config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tmp)
        tmp.close()

        try:
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name
            client.dev_mode = False

            mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")

            with self.assertRaises(requests.exceptions.ConnectionError):
                client._refresh_access_token()

            # backoff max_tries=3
            self.assertEqual(mock_post.call_count, 3)
        finally:
            os.unlink(tmp.name)


if __name__ == '__main__':
    unittest.main()
