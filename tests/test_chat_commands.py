import unittest
from unittest.mock import Mock, patch

import api_test_bootstrap  # noqa: F401
import api_server


class TestChatCommands(unittest.TestCase):
    def _mock_order(self, qty: int):
        order = Mock()
        order.quantity = qty
        order.fill_price.return_value = 123.45
        return order

    def test_buy_symbol_only_does_not_parse_symbol_as_qty(self):
        trader = Mock()
        trader.buy.return_value = self._mock_order(1)
        with patch("data_fetcher.get_live_price", return_value=123.0):
            msg = api_server._execute_chat_command("buy adanipower", trader)
        self.assertIn("Bought", msg or "")
        trader.buy.assert_called_once_with("ADANIPOWER.NS", 123.0, quantity=None)

    def test_buy_quantity_first_is_supported(self):
        trader = Mock()
        trader.buy.return_value = self._mock_order(5)
        with patch("data_fetcher.get_live_price", return_value=123.0):
            msg = api_server._execute_chat_command("buy 5 adanipower", trader)
        self.assertIn("Bought 5x ADANIPOWER.NS", msg or "")
        trader.buy.assert_called_once_with("ADANIPOWER.NS", 123.0, quantity=5)

    def test_buy_non_numeric_third_token_does_not_crash(self):
        trader = Mock()
        trader.buy.return_value = self._mock_order(1)
        with patch("data_fetcher.get_live_price", return_value=123.0):
            msg = api_server._execute_chat_command("buy adanipower now", trader)
        self.assertIn("Bought", msg or "")
        trader.buy.assert_called_once_with("ADANIPOWER.NS", 123.0, quantity=None)

    def test_sell_quantity_last_is_supported(self):
        trader = Mock()
        trader.sell.return_value = self._mock_order(3)
        with patch("data_fetcher.get_live_price", return_value=120.0):
            msg = api_server._execute_chat_command("sell adanipower 3", trader)
        self.assertIn("Sold 3x ADANIPOWER.NS", msg or "")
        trader.sell.assert_called_once_with("ADANIPOWER.NS", 120.0, quantity=3)

    def test_missing_symbol_message(self):
        trader = Mock()
        msg = api_server._execute_chat_command("buy 5", trader)
        self.assertIn("Please provide a symbol", msg or "")


if __name__ == "__main__":
    unittest.main()
