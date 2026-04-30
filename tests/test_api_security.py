import unittest

from fastapi.testclient import TestClient

import api_test_bootstrap  # noqa: F401
import api_server


class TestApiSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        api_server.API_AUTH_TOKEN = "unit-test-token"
        cls.client = TestClient(api_server.app)

    def test_trade_requires_auth(self):
        resp = self.client.post("/api/trade", json={"use_ai": False})
        self.assertEqual(401, resp.status_code)

    def test_status_requires_auth(self):
        resp = self.client.get("/api/status")
        self.assertEqual(401, resp.status_code)

    def test_chat_requires_auth(self):
        resp = self.client.post("/api/chat", json={"message": "hello", "history": []})
        self.assertEqual(401, resp.status_code)

    def test_apply_ai_signals_requires_auth(self):
        resp = self.client.post("/api/ai-signals/apply", json={"signals": []})
        self.assertEqual(401, resp.status_code)

    def test_candles_requires_auth(self):
        resp = self.client.get("/api/candles?symbol=SBIN&timeframe=1D")
        self.assertEqual(401, resp.status_code)

    def test_systemctl_rejects_invalid_action(self):
        with self.assertRaises(ValueError):
            api_server._systemctl("restart")

    def test_rate_limiter_blocks_when_exceeded(self):
        limiter = api_server.RateLimiter()
        key = "k"
        client = "127.0.0.1"
        self.assertTrue(limiter.allow(key, client, limit=2, window_sec=60))
        self.assertTrue(limiter.allow(key, client, limit=2, window_sec=60))
        self.assertFalse(limiter.allow(key, client, limit=2, window_sec=60))


if __name__ == "__main__":
    unittest.main()
