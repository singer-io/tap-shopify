import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import requests

from tap_shopify.client import (
    ShopifyClient,
    SHOPIFY_API_VERSION,
)
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError, ShopifyUnauthorizedError

class TestShopifyClientInit(unittest.TestCase):
    """Tests for ShopifyClient initialization."""

    def _make_config(self, **overrides):
        base = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "existing_token",
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

    @patch('tap_shopify.client.requests.post')
    def test_init_uses_existing_token_without_refresh(self, mock_post):
        """When an access_token is already in config, no refresh should be triggered."""
        config = self._make_config()  # has access_token='existing_token'
        path = self._write_config_file(config)
        try:
            client = ShopifyClient(path, config)
            mock_post.assert_not_called()
            self.assertEqual(client.access_token, "existing_token")
        finally:
            os.unlink(path)

    @patch('tap_shopify.client.requests.post')
    def test_init_fetches_token_when_missing(self, mock_post):
        """First run: no access_token in config, should fetch one via client credentials."""
        config = self._make_config()
        del config['access_token']
        path = self._write_config_file(config)
        try:
            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={"access_token": "fetched_token"}),
            )
            client = ShopifyClient(path, config)
            mock_post.assert_called_once()
            self.assertEqual(client.access_token, "fetched_token")
            self.assertEqual(config['access_token'], "fetched_token")
        finally:
            os.unlink(path)



class TestRefreshAccessToken(unittest.TestCase):
    """Tests for ShopifyClient._refresh_access_token."""

    def _create_client_skip_init(self, config, config_path="/tmp/dummy.json"):
        client = object.__new__(ShopifyClient)
        client.config = config
        client.config_path = config_path
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

            # Verify the correct endpoint was called (shop + .myshopify.com)
            mock_post.assert_called_once_with(
                "https://test-shop.myshopify.com/admin/oauth/access_token",
                json={
                    "client_id": "cid",
                    "client_secret": "csecret",
                    "grant_type": "client_credentials",
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )

            # Verify config file was updated (access_token only, no expiry fields)
            with open(tmp.name, 'r') as f:
                saved = json.load(f)
            self.assertEqual(saved['access_token'], "refreshed_token")
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


class TestWriteConfig(unittest.TestCase):
    """Tests for ShopifyClient._write_config."""

    def test_write_config_updates_file(self):
        """Config file should be updated with new access_token (token_expires_at not persisted)."""
        original = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "start_date": "2025-01-01T00:00:00Z",
        }
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(original, tmp)
        tmp.close()

        try:
            config = {
                **original,
                "access_token": "new_token",
            }
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name

            client._write_config()

            with open(tmp.name, 'r') as f:
                saved = json.load(f)

            self.assertEqual(saved['access_token'], "new_token")
            self.assertNotIn('token_expires_at', saved)
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
            }
            client = object.__new__(ShopifyClient)
            client.config = config
            client.config_path = tmp.name

            client._write_config()

            with open(tmp.name, 'r') as f:
                saved = json.load(f)
            self.assertEqual(saved['custom_setting'], "keep_me")
        finally:
            os.unlink(tmp.name)



class TestRefreshToken(unittest.TestCase):
    """Tests for ShopifyClient.refresh_token (reactive 401 refresh)."""

    @patch.object(ShopifyClient, '_refresh_access_token')
    def test_refresh_token_calls_underlying_refresh(self, mock_refresh):
        """refresh_token should always call _refresh_access_token."""
        client = object.__new__(ShopifyClient)
        client.config = {"shop": "test-shop", "access_token": "tok"}
        client.config_path = "/tmp/dummy.json"
        client.access_token = "tok"
        client.refresh_token()
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
        """retry_401_handler should call refresh_token and reinitialize_session."""
        from tap_shopify.streams.base import retry_401_handler

        mock_client = MagicMock()
        Context.client = mock_client

        retry_401_handler({'wait': 1, 'tries': 1})

        mock_client.refresh_token.assert_called_once()
        mock_client.reinitialize_session.assert_called_once()

    def test_retry_401_handler_no_client(self):
        """retry_401_handler should do nothing if Context.client is None."""
        from tap_shopify.streams.base import retry_401_handler

        Context.client = None
        # Should not raise
        retry_401_handler({'wait': 1, 'tries': 1})

    @patch('tap_shopify.client.requests.post')
    def test_retry_401_handler_updates_context_config_access_token(self, mock_post):
        """After retry_401_handler fires, Context.config['access_token'] must reflect
        the newly fetched token.

        This relies on ShopifyClient.config being the *same dict object* as
        Context.config (passed by reference in main()), so that
        _refresh_access_token()'s `self.config['access_token'] = ...`
        is immediately visible via Context.config['access_token'].
        """
        import tempfile, os
        from tap_shopify.streams.base import retry_401_handler

        shared_config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "old_token",
            "start_date": "2025-01-01T00:00:00Z",
        }

        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(shared_config, tmp)
        tmp.close()

        try:
            # Wire up a real ShopifyClient sharing the same config dict as Context
            client = object.__new__(ShopifyClient)
            client.config = shared_config       # same object as Context.config below
            client.config_path = tmp.name
            client.access_token = "old_token"

            Context.config = shared_config      # same dict reference
            Context.client = client

            mock_post.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={"access_token": "new_token"}),
            )

            with patch('shopify.Session'), \
                 patch('shopify.ShopifyResource.activate_session'), \
                 patch('shopify.Shop.set_timeout'):
                retry_401_handler({'wait': 1, 'tries': 1})

            # The token update in ShopifyClient must be visible through Context.config
            self.assertEqual(Context.config['access_token'], "new_token")
            self.assertEqual(client.access_token, "new_token")
        finally:
            os.unlink(tmp.name)


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
        mock_client.refresh_token.return_value = False
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


class TestMainClient(unittest.TestCase):
    """Tests for ShopifyClient wiring in main()."""

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
    def test_main_no_client_when_api_key_present(self, mock_parse_args, mock_discover):
        """main() should NOT create ShopifyClient when api_key is in config (legacy auth)."""
        from tap_shopify import main

        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "api_key": "legacy_key",
            "start_date": "2025-01-01T00:00:00Z",
        }
        mock_args.state = {}
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

    def _make_discover_args(self, extra_config=None):
        mock_args = MagicMock()
        mock_args.config = {
            "shop": "test-shop",
            "client_id": "cid",
            "client_secret": "csecret",
            "access_token": "tok",
            "start_date": "2025-01-01T00:00:00Z",
        }
        if extra_config:
            mock_args.config.update(extra_config)
        mock_args.state = {}
        mock_args.dev = False
        mock_args.discover = True
        mock_args.config_path = "/tmp/config.json"
        mock_args.catalog = None
        return mock_args

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_propagates_shopify_unauthorized_error(
        self, mock_parse_args, mock_discover, mock_client_cls
    ):
        """main() must re-raise ShopifyUnauthorizedError as-is, not wrap it in ShopifyError.

        Before the fix, ShopifyUnauthorizedError fell into `except Exception` and was
        re-raised as ShopifyError(exc) with an empty message, losing the original context.
        After the fix, a dedicated `except ShopifyUnauthorizedError` branch re-raises it
        directly so callers receive the correct type and message.
        """
        from tap_shopify import main

        mock_parse_args.return_value = self._make_discover_args()

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "tok"
        mock_client_cls.return_value = mock_client_instance

        original_error = ShopifyUnauthorizedError(
            Exception("UnauthorizedAccess"), "Invalid access token"
        )
        mock_discover.side_effect = original_error

        with self.assertRaises(ShopifyUnauthorizedError) as ctx:
            main()

        # The exception must be the original instance (not a wrapped ShopifyError)
        self.assertIsInstance(ctx.exception, ShopifyUnauthorizedError)
        self.assertNotIsInstance(ctx.exception, ShopifyError)
        self.assertIn("Invalid access token", str(ctx.exception))

    @patch('tap_shopify.ShopifyClient')
    @patch('tap_shopify.discover')
    @patch('singer.utils.parse_args')
    def test_main_unauthorized_error_message_preserved(
        self, mock_parse_args, mock_discover, mock_client_cls
    ):
        """ShopifyUnauthorizedError message must survive propagation through main().

        Previously the message was lost because the error was caught by the generic
        `except Exception` handler and re-raised as ShopifyError(exc, msg='').
        """
        from tap_shopify import main

        mock_parse_args.return_value = self._make_discover_args()

        mock_client_instance = MagicMock()
        mock_client_instance.access_token = "tok"
        mock_client_cls.return_value = mock_client_instance

        expected_message = "Invalid access token"
        mock_discover.side_effect = ShopifyUnauthorizedError(
            Exception("UnauthorizedAccess"), expected_message
        )

        try:
            main()
            self.fail("Expected ShopifyUnauthorizedError to be raised")
        except ShopifyUnauthorizedError as exc:
            self.assertIn(expected_message, str(exc))
        except ShopifyError:
            self.fail(
                "ShopifyUnauthorizedError was incorrectly wrapped as ShopifyError, "
                "losing the original message"
            )


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
            self.assertEqual(mock_post.call_count, 3)  # backoff retried twice then succeeded
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

            mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")

            with self.assertRaises(requests.exceptions.ConnectionError):
                client._refresh_access_token()

            # backoff max_tries=3
            self.assertEqual(mock_post.call_count, 3)
        finally:
            os.unlink(tmp.name)


if __name__ == '__main__':
    unittest.main()
