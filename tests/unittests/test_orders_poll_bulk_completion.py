"""
Unit tests for the 401 token-refresh-and-retry logic in
Orders.poll_bulk_completion (tap_shopify/streams/orders.py).

Scenario under test
-------------------
During a long-running GraphQL Bulk Operation poll the Shopify access token can
expire.  When that happens shopify.GraphQL().execute() raises a
urllib.error.HTTPError with status 401.  The connector must:

  1. Refresh the access token via Context.client.
  2. Reinitialize the Shopify session so subsequent calls use the new token.
  3. Update Context.config['access_token'] with the refreshed token.
  4. Retry the *same* status-check request (no bulk-operation re-submission).

If no Context.client is present (non-OAuth flow) a ShopifyAPIError is raised.
Non-401 HTTP errors must propagate unchanged.
"""

import json
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyAPIError
from tap_shopify.streams.orders import Orders

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BULK_OP_ID = "gid://shopify/BulkOperation/123"
_RESULT_URL = "https://storage.example.com/bulk_result.jsonl"


def _op_response(status, url=_RESULT_URL):
    """Build a JSON-encoded fake bulk-operation GraphQL response."""
    return json.dumps({
        "data": {
            "node": {
                "id": _BULK_OP_ID,
                "status": status,
                "errorCode": None,
                "createdAt": "2026-01-01T00:00:00Z",
                "completedAt": "2026-01-01T01:00:00Z" if status == "COMPLETED" else None,
                "objectCount": "10",
                "fileSize": "1024",
                "url": url if status == "COMPLETED" else None,
            }
        }
    })


def _http_error(code):
    """Create a urllib.error.HTTPError with the given HTTP status code."""
    return urllib.error.HTTPError(
        url="https://test.myshopify.com/admin/api/2025-07/graphql.json",
        code=code,
        msg="Unauthorized" if code == 401 else "Server Error",
        hdrs=MagicMock(get=MagicMock(return_value=None)),
        fp=None,
    )


def _make_stream():
    """Return an Orders instance with bookmarking helpers pre-mocked."""
    stream = Orders()
    stream.date_window_size = 30
    stream.update_bookmark = MagicMock()
    stream.clear_bulk_operation_state = MagicMock()
    return stream


_BOOKMARK = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _mock_client(new_token="refreshed_token"):
    """Return a mock ShopifyClient whose access_token is *new_token*."""
    client = MagicMock()
    client.access_token = new_token
    return client


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestPollBulkCompletion401Handling(unittest.TestCase):

    def setUp(self):
        self._orig_config = Context.config
        self._orig_client = Context.client
        Context.config = {
            "access_token": "original_token",
            "start_date": "2025-01-01T00:00:00Z",
        }
        Context.client = None

    def tearDown(self):
        Context.config = self._orig_config
        Context.client = self._orig_client

    # ------------------------------------------------------------------
    # Happy-path baseline
    # ------------------------------------------------------------------

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_completed_status_returns_result_url(self, mock_graphql, mock_time):
        """Immediate COMPLETED response returns the bulk result URL."""
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.return_value = _op_response("COMPLETED")

        stream = _make_stream()
        result = stream.poll_bulk_completion(
            current_bookmark=_BOOKMARK,
            bulk_op_id=_BULK_OP_ID,
        )

        self.assertEqual(result, _RESULT_URL)
        stream.update_bookmark.assert_called_once()

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_running_then_completed_polls_same_operation(self, mock_graphql, mock_time):
        """Connector loops through RUNNING before COMPLETED, calling sleep between iterations."""
        mock_time.time.side_effect = [0, 1, 62]
        mock_graphql.return_value.execute.side_effect = [
            _op_response("RUNNING"),
            _op_response("COMPLETED"),
        ]

        stream = _make_stream()
        result = stream.poll_bulk_completion(
            current_bookmark=_BOOKMARK,
            bulk_op_id=_BULK_OP_ID,
        )

        self.assertEqual(result, _RESULT_URL)
        self.assertEqual(mock_graphql.return_value.execute.call_count, 2)
        mock_time.sleep.assert_called_once_with(60)

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_failed_status_raises_shopify_api_error(self, mock_graphql, mock_time):
        """A FAILED bulk operation raises ShopifyAPIError and clears state."""
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.return_value = _op_response("FAILED")

        stream = _make_stream()
        with self.assertRaises(ShopifyAPIError):
            stream.poll_bulk_completion(
                current_bookmark=_BOOKMARK,
                bulk_op_id=_BULK_OP_ID,
            )

        stream.clear_bulk_operation_state.assert_called_once()

    # ------------------------------------------------------------------
    # 401 handling — core feature under test
    # ------------------------------------------------------------------

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_401_refreshes_token_and_retries_same_bulk_op(self, mock_graphql, mock_time):
        """
        On a 401 the connector refreshes the token, reinitializes the session,
        updates Context.config, and retries the status check for the SAME
        bulk operation — no re-submission of the bulk query.
        """
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.side_effect = [
            _http_error(401),          # first call → 401
            _op_response("COMPLETED"), # retry → success
        ]

        client = _mock_client("refreshed_token")
        Context.client = client

        stream = _make_stream()
        result = stream.poll_bulk_completion(
            current_bookmark=_BOOKMARK,
            bulk_op_id=_BULK_OP_ID,
        )

        # Token refresh and session reinit were called exactly once
        client.refresh_token.assert_called_once()
        client.reinitialize_session.assert_called_once()

        # Context.config carries the new token for subsequent calls
        self.assertEqual(Context.config["access_token"], "refreshed_token")

        # Result URL returned — polling continued on the existing bulk op
        self.assertEqual(result, _RESULT_URL)

        # execute called twice: 401 probe + successful retry
        self.assertEqual(mock_graphql.return_value.execute.call_count, 2)

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_401_mid_poll_loop_continues_polling_after_refresh(self, mock_graphql, mock_time):
        """
        401 can appear in any poll iteration.  After refresh the loop continues
        and eventually reaches COMPLETED.
        """
        mock_time.time.side_effect = [0, 1, 62]
        mock_graphql.return_value.execute.side_effect = [
            _op_response("RUNNING"),   # first poll — ok
            _http_error(401),          # second poll — token expired
            _op_response("COMPLETED"), # retry of second poll — success
        ]

        client = _mock_client("refreshed_token")
        Context.client = client

        stream = _make_stream()
        result = stream.poll_bulk_completion(
            current_bookmark=_BOOKMARK,
            bulk_op_id=_BULK_OP_ID,
        )

        client.refresh_token.assert_called_once()
        client.reinitialize_session.assert_called_once()
        self.assertEqual(Context.config["access_token"], "refreshed_token")
        self.assertEqual(result, _RESULT_URL)
        self.assertEqual(mock_graphql.return_value.execute.call_count, 3)
        mock_time.sleep.assert_called_once_with(60)

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_401_without_client_raises_shopify_api_error(self, mock_graphql, mock_time):
        """
        If Context.client is None (non-OAuth / api_key flow) and a 401 is
        received, ShopifyAPIError is raised with a descriptive message.
        """
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.side_effect = _http_error(401)
        Context.client = None  # no client available

        stream = _make_stream()
        with self.assertRaises(ShopifyAPIError) as ctx:
            stream.poll_bulk_completion(
                current_bookmark=_BOOKMARK,
                bulk_op_id=_BULK_OP_ID,
            )

        self.assertIn("no client is available", str(ctx.exception))

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_401_persists_after_retry_propagates_http_error(self, mock_graphql, mock_time):
        """
        If the retry after token refresh also returns 401, the HTTPError
        propagates to the caller — no infinite retry loop.
        """
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.side_effect = [
            _http_error(401),  # initial call
            _http_error(401),  # retry — still 401
        ]

        client = _mock_client("refreshed_token")
        Context.client = client

        stream = _make_stream()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            stream.poll_bulk_completion(
                current_bookmark=_BOOKMARK,
                bulk_op_id=_BULK_OP_ID,
            )

        self.assertEqual(ctx.exception.code, 401)
        # Refresh was still attempted once
        client.refresh_token.assert_called_once()
        client.reinitialize_session.assert_called_once()
        # execute called twice: original + one retry
        self.assertEqual(mock_graphql.return_value.execute.call_count, 2)

    # ------------------------------------------------------------------
    # Non-401 HTTP errors
    # ------------------------------------------------------------------

    @patch("tap_shopify.streams.orders.time")
    @patch("shopify.GraphQL")
    def test_non_401_http_error_propagates_without_token_refresh(self, mock_graphql, mock_time):
        """HTTP errors other than 401 (e.g. 500) are re-raised without any token refresh."""
        mock_time.time.side_effect = [0, 1]
        mock_graphql.return_value.execute.side_effect = _http_error(500)

        client = _mock_client()
        Context.client = client

        stream = _make_stream()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            stream.poll_bulk_completion(
                current_bookmark=_BOOKMARK,
                bulk_op_id=_BULK_OP_ID,
            )

        self.assertEqual(ctx.exception.code, 500)
        client.refresh_token.assert_not_called()
        client.reinitialize_session.assert_not_called()


if __name__ == "__main__":
    unittest.main()
