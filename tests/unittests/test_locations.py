import unittest
from unittest import mock
from datetime import datetime
from dateutil.tz import tzlocal
from singer import utils
from singer.utils import strptime_to_utc
from tap_shopify.context import Context

LOCATIONS_OBJECT = Context.stream_objects['locations']()

sample_response = {
  "edges": [
    {
      "node": {
        "id": "gid://shopify/Location/62030610598",
        "createdAt": "2021-04-15T20:28:09Z",
        "updatedAt": "2021-04-15T20:28:09Z"
      }
    },
    {
      "node": {
        "id": "gid://shopify/Location/64149061798",
        "createdAt": "2021-07-29T08:29:10Z",
        "updatedAt": "2023-02-15T19:30:38Z"
      }
    },
    {
      "node": {
        "id": "gid://shopify/Location/64149127334",
        "createdAt": "2021-07-29T08:30:03Z",
        "updatedAt": "2023-02-15T19:30:38Z"
      }
    },
    {
      "node": {
        "id": "gid://shopify/Location/64149160102",
        "createdAt": "2021-07-29T08:34:56Z",
        "updatedAt": "2023-02-15T19:30:37Z"
      }
    }
  ],
  "pageInfo": {
    "endCursor": "mock_cursor",
    "hasNextPage": False
  }
}

class TestLocations(unittest.TestCase):
  @mock.patch("tap_shopify.streams.base.Stream.update_bookmark")
  @mock.patch("tap_shopify.streams.base.Stream.get_bookmark")
  @mock.patch("tap_shopify.streams.locations.Locations.call_api")
  @mock.patch('tap_shopify.streams.base.utils.now', return_value=datetime(2021, 7, 30, 0, 0, tzinfo=tzlocal()))
  def test_sync(self, mock_now, mock_call_api, mock_get_bookmark, mock_update_bookmark):
    expected_sync = [
      {
        "id": "gid://shopify/Location/64149127334",
        "createdAt": "2021-07-29T08:30:03Z",
        "updatedAt": "2023-02-15T19:30:38Z"
      },
      {
        "id": "gid://shopify/Location/64149160102",
        "createdAt": "2021-07-29T08:34:56Z",
        "updatedAt": "2023-02-15T19:30:37Z"
      }
    ]
    mock_call_api.return_value = sample_response
    mock_get_bookmark.return_value = strptime_to_utc("2021-07-29T08:30:03.000000Z")

    actual_sync = list(LOCATIONS_OBJECT.sync())

    # Verify that only 2 record syncs
    self.assertEqual(actual_sync, expected_sync)
    max_bookmark = strptime_to_utc("2021-07-29T08:34:56.000000Z")

    # Verify that maximum replication key of all keys is updated as bookmark
    mock_update_bookmark.assert_called_with(utils.strftime(max_bookmark))
