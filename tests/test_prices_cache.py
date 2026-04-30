import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import api_test_bootstrap  # noqa: F401
import api_server


class TestPricesCache(unittest.TestCase):
    def setUp(self):
        api_server.API_AUTH_TOKEN = "unit-test-token"
        api_server._cached_watchlist_prices.cache_clear()
        self.client = TestClient(api_server.app)
        self.headers = {"X-API-Key": "unit-test-token"}

    def tearDown(self):
        api_server._cached_watchlist_prices.cache_clear()

    def test_prices_endpoint_reuses_5s_cache_window(self):
        calls = []

        def fake_prices():
            calls.append(1)
            return {"SBIN.NS": 123.45}

        with patch("api_server.get_watchlist_prices", side_effect=fake_prices), patch(
            "api_server.get_ttl_hash", return_value=9999
        ):
            first = self.client.get("/api/prices", headers=self.headers)
            second = self.client.get("/api/prices", headers=self.headers)

        self.assertEqual(200, first.status_code)
        self.assertEqual(200, second.status_code)
        self.assertEqual(1, len(calls))
        self.assertEqual({"status": "ok", "prices": {"SBIN.NS": 123.45}}, first.json())
        self.assertEqual(first.json(), second.json())


if __name__ == "__main__":
    unittest.main()
