import unittest
from decimal import Decimal

import api_test_bootstrap  # noqa: F401
import api_server


class _FakePosition:
    def __init__(self):
        self.quantity = 2
        self.avg_price = Decimal("100.0")

    def pnl_pct(self, current):
        current_d = Decimal(str(current))
        return ((current_d - self.avg_price) / self.avg_price) * Decimal("100")


class _FakePortfolio:
    def __init__(self):
        self.positions = {"SBIN.NS": _FakePosition()}
        self.trade_log = []


class _FakeTrader:
    def __init__(self):
        self.portfolio = _FakePortfolio()


class TestChatPortfolioText(unittest.TestCase):
    def test_handles_float_price_with_decimal_position(self):
        trader = _FakeTrader()
        summary = {
            "cash": 1000.0,
            "total_value": 1200.0,
            "total_return_pct": 20.0,
            "realized_pnl": 0.0,
        }
        prices = {"SBIN.NS": 102.5}

        text = api_server._get_chat_portfolio_text(trader, prices, summary)

        self.assertIn("POS:SBIN|2x|100.0->102.5|+2.5%", text)


if __name__ == "__main__":
    unittest.main()
