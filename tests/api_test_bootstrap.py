"""Test bootstrap: provide local stubs so api_server imports in isolated CI."""

from __future__ import annotations

import logging
import os
import sys
import types
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
os.environ.setdefault("AGENT_DIR", str(BACKEND_DIR))
os.environ.setdefault("API_AUTH_TOKEN", "unit-test-token")


def _install_config_stub() -> None:
    if "config" in sys.modules:
        return
    mod = types.ModuleType("config")
    mod.PROJECT_DIR = str(BACKEND_DIR)
    mod.INITIAL_CAPITAL = 100000.0
    mod.MAX_POSITION_SIZE_PCT = 0.10
    mod.MAX_OPEN_POSITIONS = 10
    mod.STOP_LOSS_PCT = 0.02
    mod.TAKE_PROFIT_PCT = 0.03
    mod.WATCHLIST = ["SBIN.NS", "ITC.NS"]
    sys.modules["config"] = mod


def _install_logger_stub() -> None:
    if "logger" in sys.modules:
        return
    mod = types.ModuleType("logger")
    test_logger = logging.getLogger("api_server_test")
    if not test_logger.handlers:
        test_logger.addHandler(logging.StreamHandler())
    test_logger.setLevel(logging.INFO)
    mod.logger = test_logger
    sys.modules["logger"] = mod


def _install_paper_trader_stub() -> None:
    if "paper_trader" in sys.modules:
        return
    mod = types.ModuleType("paper_trader")

    class Portfolio:
        def __init__(self):
            self.positions: dict[str, object] = {}
            self.cash = 100000.0
            self.orders: list[dict] = []

    class PaperTrader:
        def __init__(self, *args, **kwargs):
            self.portfolio = Portfolio()

        def refresh_portfolio(self):
            return None

        def check_stop_loss_take_profit(self, prices):
            return []

        def buy(self, symbol, price, quantity=None, confidence=0.0, max_position_size_pct=None, ai_signal=None):
            return None

        def sell(self, symbol, price, quantity=None):
            return None

        def get_summary(self, prices):
            return {"cash": self.portfolio.cash, "total_value": self.portfolio.cash}

    mod.PaperTrader = PaperTrader
    mod.Portfolio = Portfolio
    sys.modules["paper_trader"] = mod


def _install_data_fetcher_stub() -> None:
    if "data_fetcher" in sys.modules:
        return
    mod = types.ModuleType("data_fetcher")

    class _EmptyDataFrame:
        empty = True

    def get_watchlist_prices(watchlist=None):
        return {}

    def get_historical_data(symbol, period="30d", interval="1d"):
        return _EmptyDataFrame()

    def get_market_regime():
        return "unknown"

    def get_live_price(symbol):
        return None

    mod.get_watchlist_prices = get_watchlist_prices
    mod.get_historical_data = get_historical_data
    mod.get_market_regime = get_market_regime
    mod.get_live_price = get_live_price
    sys.modules["data_fetcher"] = mod


def _install_strategy_stub() -> None:
    if "strategy" in sys.modules:
        return
    mod = types.ModuleType("strategy")

    def get_latest_signal(symbol, df):
        return {"signal": "HOLD", "price": 0.0, "confidence": 0.0}

    def get_scored_signal(symbol, df):
        return {"symbol": symbol, "signal": "HOLD", "confidence": 0, "price": 0.0,
                "reason": "stub", "buy_score": 0, "sell_score": 0}

    mod.get_latest_signal = get_latest_signal
    mod.get_scored_signal = get_scored_signal
    sys.modules["strategy"] = mod


_install_config_stub()
_install_logger_stub()
_install_paper_trader_stub()
_install_data_fetcher_stub()
_install_strategy_stub()
