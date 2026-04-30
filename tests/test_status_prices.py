import unittest
from decimal import Decimal
from unittest.mock import patch

from fastapi.testclient import TestClient

import api_test_bootstrap  # noqa: F401
import api_server


class _Position:
    quantity = 1
    avg_price = Decimal("100.0")
    highest_price = Decimal("101.0")
    entry_time = "2026-04-21T09:30:00"

    def pnl(self, current):
        return Decimal(str(current)) - self.avg_price

    def pnl_pct(self, current):
        return ((Decimal(str(current)) - self.avg_price) / self.avg_price) * Decimal("100")


class _Portfolio:
    def __init__(self):
        self.positions = {"OLDPOS.NS": _Position()}
        self.trade_log = []


class _Trader:
    def __init__(self, *args, **kwargs):
        self.portfolio = _Portfolio()

    def get_summary(self, prices):
        return {"cash": 1000, "total_value": 1000 + prices.get("OLDPOS.NS", 0)}


class TestStatusPrices(unittest.TestCase):
    def setUp(self):
        api_server.API_AUTH_TOKEN = "unit-test-token"
        self.client = TestClient(api_server.app)
        self.headers = {"X-API-Key": "unit-test-token"}

    def test_status_fetches_prices_for_open_positions_outside_watchlist(self):
        calls = []

        def fake_prices(symbols):
            calls.append(symbols)
            return {"OLDPOS.NS": 123.45}

        with patch("api_server.PaperTrader", _Trader), patch("api_server.get_watchlist_prices", side_effect=fake_prices):
            resp = self.client.get("/api/status", headers=self.headers)

        self.assertEqual(200, resp.status_code)
        self.assertIn("OLDPOS.NS", calls[0])
        position = resp.json()["positions"][0]
        self.assertEqual("OLDPOS.NS", position["symbol"])
        self.assertEqual(123.45, position["current_price"])


if __name__ == "__main__":
    unittest.main()
