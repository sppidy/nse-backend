import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import api_test_bootstrap  # noqa: F401
import api_server


class TestApplyAiSignals(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        api_server.API_AUTH_TOKEN = "unit-test-token"
        cls.client = TestClient(api_server.app)
        cls.headers = {"X-API-Key": "unit-test-token"}

    def test_apply_ai_signals_executes_buy_with_signal_payload(self):
        class DummyTrader:
            def __init__(self):
                self.portfolio = type("P", (), {"positions": {}})()

            def refresh_portfolio(self):
                return None

            def check_stop_loss_take_profit(self, prices):
                return []

            def buy(self, symbol, price, quantity=None, confidence=0.0, max_position_size_pct=None, ai_signal=None):
                self.buy_args = {
                    "symbol": symbol,
                    "price": price,
                    "confidence": confidence,
                    "ai_signal": ai_signal,
                }
                return type("Order", (), {"quantity": 2})()

            def sell(self, symbol, price, quantity=None):
                return None

            def get_summary(self, prices):
                return {"cash": 1000, "total_value": 1000}

        dummy = DummyTrader()
        payload = {
            "signals": [
                {
                    "symbol": "SBIN.NS",
                    "signal": "BUY",
                    "confidence": 0.8,
                    "price": 100.0,
                    "stop_loss": 95.0,
                    "target": 120.0,
                    "position_size_pct": 0.1,
                }
            ],
            "min_confidence": 0.6,
        }

        with patch("api_server.PaperTrader", return_value=dummy), patch("api_server.get_watchlist_prices", return_value={"SBIN.NS": 101.0}):
            resp = self.client.post("/api/ai-signals/apply", json=payload, headers=self.headers)

        self.assertEqual(200, resp.status_code)
        body = resp.json()
        self.assertEqual("ok", body["status"])
        self.assertEqual(1, len(body["trades"]))
        self.assertEqual("BUY", body["trades"][0]["action"])
        self.assertEqual("SBIN.NS", dummy.buy_args["symbol"])
        self.assertEqual(0.8, dummy.buy_args["confidence"])
        self.assertEqual(120.0, dummy.buy_args["ai_signal"]["target"])


if __name__ == "__main__":
    unittest.main()
