"""FastAPI server for AI Trading Agent — REST + WebSocket log streaming."""

import asyncio
import json
import os
import sys
import threading
import time
import logging
import traceback
import hmac
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from functools import lru_cache
import uuid
from typing import Any

# ── Trading agent imports (package + path fallback for systemd/local runs) ──
def _ensure_agent_modules_available() -> None:
    if "config" in sys.modules:
        return

    candidates: list[Path] = []
    env_agent_dir = os.environ.get("AGENT_DIR")
    if env_agent_dir:
        candidates.append(Path(env_agent_dir).expanduser())
    candidates.append(Path(__file__).resolve().parent.parent / "ai-trading-agent")
    candidates.append(Path(__file__).resolve().parent.parent.parent / "ai-trading-agent")

    for candidate in candidates:
        if not candidate.exists():
            continue
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)
        try:
            __import__("config")
            return
        except ModuleNotFoundError:
            continue


_ensure_agent_modules_available()
import config
from paper_trader import PaperTrader, Portfolio
from data_fetcher import get_watchlist_prices, get_historical_data, get_market_regime
from strategy import get_scored_signal
from logger import logger

# ── Job Queue state with TTL cleanup ──
JOB_TTL_SECONDS = 600  # Jobs expire after 10 minutes
JOB_MAX_COUNT = 200    # Hard cap on stored jobs
RATE_LIMIT_WINDOW_SEC = 60
RATE_LIMIT_SCAN_START = 8
RATE_LIMIT_SCAN_STATUS = 45
RATE_LIMIT_CHAT_START = 8
RATE_LIMIT_CHAT_STATUS = 45
RATE_LIMIT_TRADE = 12

# Serialize portfolio read-modify-write across all trade endpoints to prevent
# concurrent overdraw. PaperTrader reads cash, decides, then writes — two
# requests racing would both pass the cash check.
_PORTFOLIO_LOCK = threading.Lock()

API_AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN", "change-me")
# Fail-fast in production: refuse to start with default/empty token unless explicitly allowed
# for local dev by setting ALLOW_INSECURE_API_TOKEN=1.
if (not API_AUTH_TOKEN or API_AUTH_TOKEN == "change-me") and os.environ.get("ALLOW_INSECURE_API_TOKEN", "0") != "1":
    raise RuntimeError(
        "API_AUTH_TOKEN is unset or using the default 'change-me'. Set a strong token in the "
        "environment, or export ALLOW_INSECURE_API_TOKEN=1 for local development."
    )
TRUSTED_ORIGINS = [
    o.strip()
    for o in os.environ.get(
        "TRUSTED_ORIGINS",
        "http://localhost:8000,http://127.0.0.1:8000",
    ).split(",")
    if o.strip()
]


class JobStore:
    """Thread-safe job store with automatic TTL cleanup."""

    def __init__(self, ttl: int = JOB_TTL_SECONDS, max_count: int = JOB_MAX_COUNT):
        self._jobs: dict[str, dict[str, Any]] = {}
        self._timestamps: dict[str, float] = {}
        self._lock = threading.Lock()
        self._ttl = ttl
        self._max_count = max_count

    def create(self, job_id: str, initial: dict[str, Any]) -> None:
        with self._lock:
            self._cleanup_expired()
            self._jobs[job_id] = initial
            self._timestamps[job_id] = time.time()

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            if job_id not in self._jobs:
                return None
            if time.time() - self._timestamps[job_id] > self._ttl:
                del self._jobs[job_id]
                del self._timestamps[job_id]
                return None
            return self._jobs[job_id]

    def update(self, job_id: str, updates: dict[str, Any]) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].update(updates)
                self._timestamps[job_id] = time.time()  # Refresh TTL on update

    def _cleanup_expired(self) -> None:
        """Remove expired jobs. Called while lock is held."""
        now = time.time()
        expired = [jid for jid, ts in self._timestamps.items() if now - ts > self._ttl]
        for jid in expired:
            self._jobs.pop(jid, None)
            self._timestamps.pop(jid, None)
        # Hard cap: if still too many, remove oldest
        if len(self._jobs) > self._max_count:
            sorted_ids = sorted(self._timestamps, key=self._timestamps.get)
            to_remove = sorted_ids[: len(self._jobs) - self._max_count]
            for jid in to_remove:
                self._jobs.pop(jid, None)
                self._timestamps.pop(jid, None)


jobs = JobStore()


class RateLimiter:
    """Simple in-memory fixed-window rate limiter by (key, client)."""

    def __init__(self):
        self._events: dict[tuple[str, str], list[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str, client_id: str, limit: int, window_sec: int = RATE_LIMIT_WINDOW_SEC) -> bool:
        now = time.time()
        cutoff = now - window_sec
        bucket = (key, client_id)
        with self._lock:
            arr = self._events.get(bucket, [])
            arr = [ts for ts in arr if ts >= cutoff]
            if len(arr) >= limit:
                self._events[bucket] = arr
                return False
            arr.append(now)
            self._events[bucket] = arr
            return True


rate_limiter = RateLimiter()


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _client_id(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not API_AUTH_TOKEN:
        raise HTTPException(status_code=503, detail="API auth not configured")
    if API_AUTH_TOKEN == "change-me":
        logger.warning("API_AUTH_TOKEN is using default value; update for real deployment security.")
    if not x_api_key or not hmac.compare_digest(x_api_key, API_AUTH_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _is_api_key_valid(x_api_key: str | None) -> tuple[bool, int, str]:
    if not API_AUTH_TOKEN:
        return False, 503, "API auth not configured"
    if API_AUTH_TOKEN == "change-me":
        logger.warning("API_AUTH_TOKEN is using default value; update for real deployment security.")
    if not x_api_key or not hmac.compare_digest(x_api_key, API_AUTH_TOKEN):
        return False, 401, "Unauthorized"
    return True, 200, "ok"


def _check_rate(request: Request, key: str, limit: int):
    cid = _client_id(request)
    if not rate_limiter.allow(key=key, client_id=cid, limit=limit):
        raise HTTPException(status_code=429, detail="Too many requests")


def _internal_error(public_message: str, e: Exception):
    logger.error(f"{public_message}: {e}")
    logger.debug(traceback.format_exc())
    raise HTTPException(status_code=500, detail=public_message)


def get_ttl_hash(seconds=300):
    return round(time.time() / seconds)

@lru_cache(maxsize=1)
def _cached_market_regime(ttl_hash):
    return get_market_regime()


PRICES_CACHE_SECONDS = max(1, int(os.getenv("PRICES_CACHE_SECONDS", "5")))


@lru_cache(maxsize=1)
def _cached_watchlist_prices(ttl_hash):
    return get_watchlist_prices() or {}


# ── Log streaming infrastructure ──
LOG_FILE = os.path.join(config.PROJECT_DIR, "logs", "trading_agent.log")


class AsyncQueueHandler(logging.Handler):
    def __init__(self, queue: asyncio.Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            msg = self.format(record)
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.queue.put_nowait, msg)
            except RuntimeError:
                pass
        except Exception:
            self.handleError(record)

class LogBroadcaster:
    """
    Broadcasts log lines to connected WebSocket clients from two sources:

    1. In-process Python logger — catches anything this API server logs.
    2. On-disk tail of trading_agent.log — catches output from the autopilot
       process (separate systemd unit) that only writes to the file.
    """

    def __init__(self):
        self.clients: list[WebSocket] = []
        self.queue: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None
        self._tail_task: asyncio.Task | None = None
        self.handler = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, message: str):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def process_queue(self):
        try:
            while True:
                msg = await self.queue.get()
                payload = json.dumps({
                    "type": "log",
                    "message": msg,
                    "timestamp": datetime.now().isoformat(),
                })
                await self.broadcast(payload)
        except asyncio.CancelledError:
            pass

    async def tail_log_file(self, path: str):
        """Stream new lines appended to `path` into the broadcast queue.

        Handles the file not existing yet (poll), log rotation (inode change
        or file shrink → reopen from start), and partial writes (buffers until
        newline).
        """
        try:
            last_inode: int | None = None
            fh = None
            buf = ""
            while True:
                try:
                    st = os.stat(path)
                except FileNotFoundError:
                    if fh is not None:
                        try: fh.close()
                        except Exception: pass
                        fh = None
                    await asyncio.sleep(1.0)
                    continue

                if fh is None or st.st_ino != last_inode:
                    if fh is not None:
                        try: fh.close()
                        except Exception: pass
                    fh = open(path, "r", encoding="utf-8", errors="replace")
                    last_inode = st.st_ino
                    # Start from end so we don't dump the whole history to new
                    # clients; users who want history call /api/logs/recent.
                    fh.seek(0, os.SEEK_END)

                # If the file was rotated in place (truncated), reset.
                try:
                    if fh.tell() > st.st_size:
                        fh.seek(0, os.SEEK_END)
                except Exception:
                    pass

                chunk = fh.read()
                if chunk:
                    buf += chunk
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        line = line.rstrip("\r")
                        if line and self.queue is not None:
                            await self.queue.put(line)
                else:
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.queue is not None:
                await self.queue.put(f"[log tail] error: {e}")

    def start(self, loop: asyncio.AbstractEventLoop):
        self.queue = asyncio.Queue()
        self.handler = AsyncQueueHandler(self.queue)
        self.handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(self.handler)
        self._task = loop.create_task(self.process_queue())
        self._tail_task = loop.create_task(self.tail_log_file(LOG_FILE))

    def stop(self):
        if self._task:
            self._task.cancel()
        if self._tail_task:
            self._tail_task.cancel()
        if self.handler:
            logger.removeHandler(self.handler)


log_broadcaster = LogBroadcaster()

# ── Autopilot — backed by systemd service ──
AUTOPILOT_SERVICE = os.environ.get("AUTOPILOT_SERVICE", "ai-trading-agent.service")
AUTOPILOT_LOG = os.path.join(config.PROJECT_DIR, "logs", "autopilot.log")


import subprocess


def _systemctl(action: str) -> subprocess.CompletedProcess:
    """Run a systemctl command against the autopilot service."""
    allowed_actions = {"start", "stop", "show"}
    if action not in allowed_actions:
        raise ValueError(f"Unsupported systemctl action: {action}")
    if not AUTOPILOT_SERVICE.endswith(".service") or any(c.isspace() for c in AUTOPILOT_SERVICE):
        raise ValueError("Invalid AUTOPILOT_SERVICE value")
    return subprocess.run(
        ["sudo", "/usr/bin/systemctl", action, AUTOPILOT_SERVICE],
        capture_output=True, text=True, timeout=15,
    )


def _get_service_status() -> dict:
    """Query systemd for the real autopilot service state."""
    try:
        result = subprocess.run(
            ["sudo", "/usr/bin/systemctl", "show", AUTOPILOT_SERVICE,
             "--property=ActiveState,SubState,MainPID,ExecMainStartTimestamp"],
            capture_output=True, text=True, timeout=10,
        )
        props = {}
        for line in result.stdout.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()

        running = props.get("ActiveState") == "active" and props.get("SubState") == "running"
        started_at = props.get("ExecMainStartTimestamp", "")

        # Read cycle count from persistent file (written by autopilot each cycle)
        cycle = 0
        cycle_file = os.path.join(config.PROJECT_DIR, "logs", "cycle_count.txt")
        if running:
            try:
                with open(cycle_file, "r") as f:
                    cycle = int(f.read().strip())
            except Exception:
                pass

        return {
            "running": running,
            "cycle": cycle,
            "started_at": started_at if running else None,
            "interval": 15,
            "pid": int(props.get("MainPID", 0)),
        }
    except Exception:
        return {"running": False, "cycle": 0, "started_at": None, "interval": 15, "pid": 0}


from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

# ── FastAPI app ──
@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    log_broadcaster.start(loop)
    yield
    log_broadcaster.stop()


app = FastAPI(
    title="AI Trading Agent API",
    version="1.0.0",
    lifespan=lifespan,
)

# Serve static files for the frontend
frontend_path = Path(__file__).resolve().parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/dashboard", StaticFiles(directory=str(frontend_path), html=True), name="frontend")
    
    @app.get("/")
    async def root_redirect():
        return RedirectResponse(url="/dashboard")
else:
    logger.warning(f"Frontend directory not found at {frontend_path}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=TRUSTED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def api_key_http_guard(request: Request, call_next):
    # Enforce API key for all API routes.
    if request.url.path.startswith("/api/") and request.method != "OPTIONS":
        ok, status, message = _is_api_key_valid(request.headers.get("X-API-Key"))
        if not ok:
            return JSONResponse(status_code=status, content={"detail": message})
    return await call_next(request)


# ── Pydantic models ──
class AutopilotRequest(BaseModel):
    interval: int = 15
    use_ai: bool = True
    force: bool = False


class TradeRequest(BaseModel):
    use_ai: bool = True


class OrderRequest(BaseModel):
    """Manual buy/sell order from a client UI (desktop / Android)."""
    symbol: str
    side: str                           # "BUY" or "SELL"
    quantity: int | None = None         # None on buy → size by remaining cash / max pos;
                                        # None on sell → close entire position.
    price: float | None = None          # optional limit; when missing we use live price
    portfolio: str = "main"             # NSE only: "main" or "eval"

    @field_validator("side")
    @classmethod
    def side_valid(cls, v: str) -> str:
        up = (v or "").strip().upper()
        if up not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        return up

    @field_validator("symbol")
    @classmethod
    def symbol_nonblank(cls, v: str) -> str:
        s = (v or "").strip().upper()
        if not s:
            raise ValueError("symbol required")
        return s


class SignalPayload(BaseModel):
    symbol: str
    signal: str = "HOLD"
    confidence: float = 0.0
    price: float | None = None
    position_size_pct: float | None = None
    stop_loss: float | None = None
    target: float | None = None
    reason: str | None = None


class ApplySignalsRequest(BaseModel):
    signals: list[SignalPayload]
    min_confidence: float = 0.6


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] | None = None

    @field_validator("message")
    @classmethod
    def message_not_too_long(cls, v: str) -> str:
        if len(v) > 2000:
            raise ValueError("Message must be 2000 characters or fewer")
        return v


TIMEFRAME_TO_YF: dict[str, tuple[str, str]] = {
    "5m":  ("5d",   "5m"),
    "15m": ("1mo",  "15m"),
    "1h":  ("3mo",  "60m"),
    "1d":  ("1y",   "1d"),
    "1w":  ("5y",   "1wk"),
    "1mo": ("10y",  "1mo"),
    "1y":  ("1y",   "1d"),
}


def _normalize_symbol(symbol: str) -> str:
    cleaned = (symbol or "").strip().upper()
    if not cleaned:
        raise HTTPException(status_code=400, detail="symbol is required")
    if not cleaned.endswith(".NS"):
        cleaned = f"{cleaned}.NS"
    return cleaned


def _resolve_timeframe(timeframe: str) -> tuple[str, str]:
    # Normalise case so '1D', '1d', '1M' and '1mo' all resolve — the legacy
    # frontend mixed cases, and it's easier to be lenient here than coordinate
    # every client.
    raw = (timeframe or "").strip()
    tf = raw.lower()
    # legacy uppercase alias: '1M' used to mean 1 month, now '1mo'
    if tf == "1M".lower() and "1M".lower() not in TIMEFRAME_TO_YF:
        tf = "1mo"
    if tf not in TIMEFRAME_TO_YF:
        supported = ", ".join(TIMEFRAME_TO_YF.keys())
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported timeframe '{raw}'. Use one of: {supported}",
        )
    return TIMEFRAME_TO_YF[tf]


# ── Chat engine (reuses chat.py logic without Rich UI) ──

_chat_client = None

# Per-provider circuit breaker: if a provider fails, don't retry it until this
# timestamp. Keyed by provider name. Prevents thundering-herd when all
# providers are simultaneously rate-limited.
_PROVIDER_COOLDOWN: dict[str, float] = {}
_PROVIDER_FAIL_COUNT: dict[str, int] = {}


def _provider_available(name: str) -> bool:
    until = _PROVIDER_COOLDOWN.get(name, 0)
    return time.time() >= until


def _mark_provider_failed(name: str, err: Exception) -> None:
    err_str = str(err).lower()
    # Rate-limit / quota errors → longer cooldown with exponential backoff
    is_rate = any(k in err_str for k in ("429", "rate_limit", "quota", "too many"))
    fails = _PROVIDER_FAIL_COUNT.get(name, 0) + 1
    _PROVIDER_FAIL_COUNT[name] = fails
    if is_rate:
        cooldown = min(300, 30 * (2 ** min(fails - 1, 3)))  # 30s, 60s, 120s, 240s, cap 300s
    else:
        cooldown = min(60, 10 * fails)  # generic failure, shorter cooldown
    _PROVIDER_COOLDOWN[name] = time.time() + cooldown
    logger.info(f"  [API] Provider '{name}' cooling down for {cooldown}s (fail #{fails})")


def _mark_provider_ok(name: str) -> None:
    _PROVIDER_FAIL_COUNT.pop(name, None)
    _PROVIDER_COOLDOWN.pop(name, None)

def _get_chat_client():
    """Get Gemini client for chat fallback. Returns None if no key."""
    global _chat_client
    if _chat_client is None:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(config.PROJECT_DIR, ".env"))
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            return None
        from google import genai
        _chat_client = genai.Client(api_key=api_key)
    return _chat_client



def _chat_call_copilot(prompt: str) -> str:
    """Call Claude Haiku via Copilot proxy (sync)."""
    import requests as _requests
    proxy_url = os.environ.get("COPILOT_PROXY_URL", "http://localhost:4141/v1")
    models = ["claude-haiku", "gpt-4o-mini"]
    for model in models:
        try:
            resp = _requests.post(
                f"{proxy_url}/chat/completions",
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.4,
                    "max_tokens": 1024,
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception:
            continue
    raise ValueError("Copilot proxy unavailable")


def _chat_call_ollama(prompt: str) -> str:
    """Call Ollama (OpenAI-compatible) on self-hosted server (sync)."""
    import requests as _requests
    base_url = os.environ.get("OLLAMA_BASE_URL", "")
    model = os.environ.get("OLLAMA_MODEL", "nemotron-3-nano:4b")
    try:
        resp = _requests.post(
            f"{base_url}/chat/completions",
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.4,
                "max_tokens": 1024,
            },
            headers={"Content-Type": "application/json"},
            timeout=240,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        raise ValueError(f"Ollama unavailable: {e}")


def _chat_call_openrouter(prompt: str) -> str:
    """Call OpenRouter for chat (sync)."""
    import requests as _requests
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not set")
    models = ["meta-llama/llama-3.3-70b-instruct", "mistralai/mistral-small-3.1-24b-instruct"]
    for model in models:
        try:
            resp = _requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.4,
                    "max_tokens": 1024,
                },
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception:
            continue
    raise ValueError("All OpenRouter models exhausted")


def _chat_call_groq(prompt: str) -> str:
    """Call Groq for chat (sync, fast)."""
    import time as _time
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        raise ValueError("GROQ_API_KEY not set")
    from groq import Groq
    client = Groq(api_key=groq_key)
    models = [
        "llama-3.3-70b-versatile",
        "qwen/qwen3-32b",
        "llama-3.1-8b-instant",
    ]
    # One attempt per model — cascade-level circuit breaker handles retries/backoff.
    for model in models:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
                max_tokens=1024,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            err = str(e).lower()
            if "404" in err or "not_found" in err or "decommissioned" in err:
                continue  # try next model
            raise
    raise ValueError("All Groq models exhausted")


def _chat_call_cloudflare(prompt: str) -> str:
    """Call Cloudflare Workers AI for chat (sync)."""
    import requests as _requests
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not api_token:
        raise ValueError("CLOUDFLARE_API_TOKEN not set")
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
    if not account_id:
        raise ValueError("CLOUDFLARE_ACCOUNT_ID not set")
    models = [
        "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
        "@cf/mistralai/mistral-small-3.1-24b-instruct",
    ]
    for model in models:
        try:
            resp = _requests.post(
                f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1/chat/completions",
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.4,
                    "max_tokens": 1024,
                },
                headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception:
            continue
    raise ValueError("All Cloudflare models exhausted")


def _chat_call_gemini(prompt: str) -> str:
    """Call Gemini for chat (fallback)."""
    import time as _time
    client = _get_chat_client()
    if client is None:
        raise ValueError("GEMINI_API_KEY not set — Gemini fallback unavailable")
    models = [
        "gemma-4-31b-it",
        "gemma-4-26b-a4b-it",
        "gemini-2.5-flash",
        "gemini-2.0-flash",
    ]
    for model in models:
        for attempt in range(3):
            try:
                response = client.models.generate_content(model=model, contents=prompt)
                return response.text.strip()
            except Exception as e:
                err = str(e)
                if (
                    "429" in err
                    or "RESOURCE_EXHAUSTED" in err
                    or "503" in err
                    or "UNAVAILABLE" in err.upper()
                ):
                    if attempt < 2:
                        _time.sleep(5 * (attempt + 2))
                    else:
                        break
                elif "404" in err or "NOT_FOUND" in err:
                    break
                else:
                    raise
    return "All AI models are rate-limited right now. Try again in a minute."


def _build_chat_system_prompt() -> str:
    trailing_info = (
        f"Dynamic trailing enabled: SL tightens as profit grows (lock scale {config.TRAILING_PROFIT_LOCK_SCALE}). "
        f"SL range: {config.MIN_STOP_LOSS_PCT*100}%-{config.MAX_STOP_LOSS_PCT*100}%, "
        f"TP range: {config.MIN_TAKE_PROFIT_PCT*100}%-{config.MAX_TAKE_PROFIT_PCT*100}%."
    ) if config.DYNAMIC_TRAILING_ENABLED else (
        f"Fixed SL:{config.STOP_LOSS_PCT*100}%, TP:{config.TAKE_PROFIT_PCT*100}%."
    )
    return f"""You are an AI trading assistant for an NSE paper trading system. Date: {datetime.now().strftime('%d-%b-%Y %I:%M%p')} IST. Market hours: 9:15AM-3:30PM Mon-Fri.

PORTFOLIO CONFIG:
- Capital: Rs.{config.INITIAL_CAPITAL} | Max {config.MAX_POSITION_SIZE_PCT*100}% per position | Max {config.MAX_OPEN_POSITIONS} open positions
- {trailing_info}
- Confidence threshold: >60% to trigger trades
- Slippage: {config.SLIPPAGE_PCT*100}% simulated | Brokerage: Rs.{config.BROKERAGE_PER_ORDER}/order
- Watchlist: {', '.join(s.replace('.NS','') for s in config.WATCHLIST)}

SYSTEM CAPABILITIES (what this trading bot can do):
1. **AI Scan**: AI (Groq/Gemini) analyzes all watchlist stocks using technicals + news sentiment, returns BUY/SELL/HOLD signals with confidence scores, stop-loss, and target prices.
2. **Rule Scan**: RSI(14) + EMA(9/21) crossover based signals — no AI, pure technical analysis.
3. **Buy/Sell execution**: Paper trades with simulated slippage and position sizing. User can type "buy SYMBOL [qty]" or "sell SYMBOL [qty]" to execute directly.
4. **Trailing stop-loss**: Tracks highest price per position. Stop-loss tightens automatically as profit increases (locks in gains). Confidence-adjusted — high-confidence trades get tighter stops.
5. **AI-provided stop-loss/targets**: AI can set custom SL/target per trade that override defaults.
6. **News sentiment**: Aggregates from 5 sources (Google News, Yahoo Finance, Twitter/X, Economic Times, Moneycontrol). Bearish news reduces confidence, bullish boosts it.
7. **ML predictions**: GradientBoosting model trained on trade history (observing mode, <55% accuracy — used as supplementary signal, not primary).
8. **Trade journal & lessons**: Every trade is logged. System learns from wins/losses to avoid repeating bad patterns.
9. **Autopilot mode**: Runs AI trade cycles automatically every N minutes during market hours. Can be started/stopped from the app.
10. **Portfolio management**: Tracks positions, P&L (realized + unrealized), cash, trade history, and performance stats (win rate, avg win/loss).

RESPONSE RULES:
- Be concise (under 150 words). Use Rs. for currency.
- Use ONLY the provided data (portfolio, market, news, history) — do not hallucinate prices or positions.
- For buy/sell recommendations, tell the user the exact command: `buy SYMBOL [qty]` or `sell SYMBOL [qty]`.
- Always mention risks and stop-loss levels when suggesting trades.
- Use markdown for formatting.
- When discussing signals, reference the confidence score and reason.
- If user asks about capabilities, explain what the system can do from the list above."""


def _get_chat_portfolio_text(trader, prices, summary) -> str:
    text = f"Cash:Rs.{summary['cash']:.0f},Value:Rs.{summary['total_value']:.0f},Ret:{summary['total_return_pct']:+.2f}%,P&L:Rs.{summary['realized_pnl']:.1f}\n"
    if trader.portfolio.positions:
        for sym, pos in trader.portfolio.positions.items():
            current = _to_decimal(prices.get(sym, pos.avg_price))
            avg_price = _to_decimal(pos.avg_price)
            pnl_pct = _to_decimal(pos.pnl_pct(current))
            highest = float(pos.highest_price) if hasattr(pos, 'highest_price') else float(current)
            sl_pct = float(pos.dynamic_stop_loss_pct) * 100 if hasattr(pos, 'dynamic_stop_loss_pct') else config.STOP_LOSS_PCT * 100
            tp_pct = float(pos.dynamic_take_profit_pct) * 100 if hasattr(pos, 'dynamic_take_profit_pct') else config.TAKE_PROFIT_PCT * 100
            text += (
                f"POS:{sym.replace('.NS','')}|{pos.quantity}x|"
                f"{float(avg_price):.1f}->{float(current):.1f}|{float(pnl_pct):+.1f}%|"
                f"Peak:{highest:.1f}|SL:{sl_pct:.1f}%|TP:{tp_pct:.1f}%\n"
            )
    if trader.portfolio.trade_log:
        for t in trader.portfolio.trade_log[-3:]:
            text += f"CLOSED:{t['symbol'].replace('.NS','')}|Rs.{t['pnl']:.1f}|{t['pnl_pct']:+.1f}%\n"
    return text


def _get_chat_market_snapshot() -> str:
    from strategy import add_indicators
    lines = []
    for symbol in config.WATCHLIST:
        try:
            df = get_historical_data(symbol, period="30d", interval="1d")
            if df.empty:
                continue
            df = add_indicators(df)
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else latest
            chg = ((latest["Close"] - prev["Close"]) / prev["Close"]) * 100
            ema = "B" if latest["ema_short"] > latest["ema_long"] else "X"
            lines.append(f"{symbol.replace('.NS','')}|{latest['Close']:.1f}|{chg:+.1f}%|RSI:{latest['rsi']:.0f}|EMA:{ema}")
        except Exception:
            pass
    return "\n".join(lines) if lines else "No data."


def _execute_chat_command(user_input: str, trader: PaperTrader) -> str | None:
    lower = user_input.strip().lower()
    if lower.startswith("buy "):
        parts = lower.split()
        if len(parts) >= 2:
            qty = None
            symbol_token = parts[1]
            if symbol_token.isdigit():
                qty = int(symbol_token)
                symbol_token = parts[2] if len(parts) >= 3 else ""
            elif len(parts) >= 3 and parts[2].isdigit():
                qty = int(parts[2])
            symbol = symbol_token.strip(",;").upper()
            if not symbol:
                return "Please provide a symbol. Example: buy SBIN 10"
            if not symbol.endswith(".NS"):
                symbol += ".NS"
            from data_fetcher import get_live_price
            price = get_live_price(symbol)
            if price is None or price <= 0:
                return f"Could not fetch price for {symbol}."
            order = trader.buy(symbol, price, quantity=qty)
            if order:
                return f"Bought {order.quantity}x {symbol} @ Rs.{order.fill_price():.2f}"
            return f"Could not buy {symbol}. Check funds or position limits."
    if lower.startswith("sell "):
        parts = lower.split()
        if len(parts) >= 2:
            qty = None
            symbol_token = parts[1]
            if symbol_token.isdigit():
                qty = int(symbol_token)
                symbol_token = parts[2] if len(parts) >= 3 else ""
            elif len(parts) >= 3 and parts[2].isdigit():
                qty = int(parts[2])
            symbol = symbol_token.strip(",;").upper()
            if not symbol:
                return "Please provide a symbol. Example: sell SBIN 10"
            if not symbol.endswith(".NS"):
                symbol += ".NS"
            from data_fetcher import get_live_price
            price = get_live_price(symbol)
            if price is None or price <= 0:
                return f"Could not fetch price for {symbol}."
            order = trader.sell(symbol, price, quantity=qty)
            if order:
                return f"Sold {order.quantity}x {symbol} @ Rs.{order.fill_price():.2f}"
            return f"Could not sell {symbol}. Check if you hold it."
    return None


def _get_autopilot_snapshot() -> dict:
    """Return real autopilot status from systemd."""
    return _get_service_status()


def _apply_signals(
    trader: PaperTrader,
    prices: dict[str, float],
    signals: list[dict],
    min_confidence: float,
) -> list[dict]:
    trades_executed = []
    sorted_signals = sorted(signals, key=lambda s: str(s.get("symbol", "")))
    for sig in sorted_signals:
        trader.refresh_portfolio()
        symbol = str(sig.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        signal = str(sig.get("signal", "HOLD")).upper()
        confidence = float(sig.get("confidence", 0) or 0)
        if confidence < min_confidence:
            continue

        fallback_price = sig.get("price", 0)
        price = prices.get(symbol, fallback_price)
        try:
            price = float(price)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            continue

        if signal == "BUY" and symbol not in trader.portfolio.positions:
            pos_size = sig.get("position_size_pct")
            if pos_size is None:
                pos_size = confidence
            try:
                pos_size = float(pos_size)
            except (TypeError, ValueError):
                pos_size = confidence
            pos_size = max(0.01, min(pos_size, 1.0))
            order = trader.buy(
                symbol,
                price,
                confidence=confidence,
                max_position_size_pct=pos_size,
                ai_signal=sig,
            )
            if order:
                trades_executed.append({"action": "BUY", "symbol": symbol, "price": price, "quantity": order.quantity})
        elif signal == "SELL" and symbol in trader.portfolio.positions:
            order = trader.sell(symbol, price)
            if order:
                trades_executed.append({"action": "SELL", "symbol": symbol, "price": price, "quantity": order.quantity})
    return trades_executed


def _unique_symbols(symbols) -> list[str]:
    seen = set()
    out = []
    for symbol in symbols:
        cleaned = str(symbol or "").upper().strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _get_prices_for_trader(trader: PaperTrader | None = None) -> dict[str, float]:
    symbols = list(getattr(config, "WATCHLIST", []))
    if trader is not None:
        symbols.extend(getattr(trader.portfolio, "positions", {}).keys())
    return get_watchlist_prices(_unique_symbols(symbols)) or {}


# ── REST Endpoints ──

@app.get("/api/status")
def get_status(portfolio: str = "main"):
    """Get portfolio status and summary. `?portfolio=main|eval` selects which."""
    if portfolio not in getattr(config, "PORTFOLIOS", {"main": 0}):
        raise HTTPException(status_code=400, detail=f"Unknown portfolio '{portfolio}'")
    try:
        trader = PaperTrader(name=portfolio)
        prices = _get_prices_for_trader(trader)
        summary = trader.get_summary(prices)

        # Add open positions detail
        positions = []
        for sym, pos in trader.portfolio.positions.items():
            current = _to_decimal(prices.get(sym, pos.avg_price))
            positions.append({
                "symbol": sym,
                "quantity": pos.quantity,
                "avg_price": round(float(pos.avg_price), 2),
                "current_price": round(float(current), 2),
                "pnl": round(float(pos.pnl(current)), 2),
                "pnl_pct": round(float(pos.pnl_pct(current)), 2),
                "highest_price": round(float(pos.highest_price), 2),
                "entry_time": pos.entry_time,
            })

        # Recent trades
        recent_trades = trader.portfolio.trade_log[-20:] if trader.portfolio.trade_log else []

        return {
            "status": "ok",
            "portfolio": portfolio,
            "portfolios_available": list(getattr(config, "PORTFOLIOS", {"main": 0}).keys()),
            "summary": summary,
            "positions": positions,
            "recent_trades": recent_trades,
            "watchlist": config.WATCHLIST,
            "autopilot": _get_autopilot_snapshot(),
        }
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to get status", e)


@app.get("/api/prices")
def get_prices():
    """Get current watchlist prices."""
    try:
        prices = _cached_watchlist_prices(get_ttl_hash(PRICES_CACHE_SECONDS))
        return {"status": "ok", "prices": prices}
    except Exception as e:
        _internal_error("Failed to get prices", e)


@app.get("/api/candles")
def get_candles(
    symbol: str,
    timeframe: str = "1h",
    limit: int = 300,
):
    """Get Yahoo Finance OHLC candles for a symbol/timeframe."""
    try:
        yf_symbol = _normalize_symbol(symbol)
        period, interval = _resolve_timeframe(timeframe)
        capped_limit = max(50, min(limit, 1000))

        df = get_historical_data(yf_symbol, period=period, interval=interval)
        if df.empty:
            return {"status": "ok", "symbol": yf_symbol, "timeframe": timeframe, "candles": []}

        rows = df.tail(capped_limit)
        candles = []
        for idx, row in rows.iterrows():
            candles.append(
                {
                    "t": idx.isoformat(),
                    "o": round(float(row["Open"]), 2),
                    "h": round(float(row["High"]), 2),
                    "l": round(float(row["Low"]), 2),
                    "c": round(float(row["Close"]), 2),
                    "v": int(row["Volume"]) if row.get("Volume") is not None else 0,
                }
            )

        return {"status": "ok", "symbol": yf_symbol, "timeframe": timeframe, "candles": candles}
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to get candles", e)


@app.get("/api/market-regime")
async def get_regime():
    """Get current market regime (BULL/BEAR/NEUTRAL) with caching."""
    try:
        regime = await asyncio.to_thread(_cached_market_regime, get_ttl_hash(300))
        return {"status": "ok", "regime": regime}
    except Exception as e:
        _internal_error("Failed to get market regime", e)


@app.get("/api/scan")
def run_scan():
    """Run rule-based scan on watchlist using multi-indicator scored signals."""
    try:
        signals = []
        for symbol in config.WATCHLIST:
            df = get_historical_data(symbol, period="60d", interval="1d")
            if df.empty:
                continue
            sig = get_scored_signal(symbol, df)
            price = sig.get("price", 0)
            signal_dir = sig.get("signal", "HOLD")

            # Compute SL/target based on config + ATR if available
            stop_loss = None
            target = None
            if signal_dir in ("BUY", "SELL") and price and price > 0:
                indicators = sig.get("indicators", {})
                # Use dynamic SL/TP range scaled by confidence
                conf = sig.get("confidence", 0)
                sl_pct = config.STOP_LOSS_PCT
                tp_pct = config.TAKE_PROFIT_PCT
                if config.DYNAMIC_TRAILING_ENABLED and conf > 0:
                    # Higher confidence => tighter SL, wider TP
                    sl_pct = max(config.MIN_STOP_LOSS_PCT,
                                 config.STOP_LOSS_PCT - (conf - 0.5) * config.TRAILING_CONFIDENCE_SCALE * 0.01)
                    tp_pct = min(config.MAX_TAKE_PROFIT_PCT,
                                 config.TAKE_PROFIT_PCT + (conf - 0.5) * 0.02)

                if signal_dir == "BUY":
                    stop_loss = round(price * (1 - sl_pct), 2)
                    target = round(price * (1 + tp_pct), 2)
                else:  # SELL
                    stop_loss = round(price * (1 + sl_pct), 2)
                    target = round(price * (1 - tp_pct), 2)

            # Compute position size like the trade path does
            position_size_pct = None
            if signal_dir in ("BUY", "SELL"):
                position_size_pct = round(min(max(sig.get("confidence", 0), 0.01), 1.0), 2)

            signals.append({
                "symbol": sig["symbol"],
                "signal": signal_dir,
                "price": price,
                "confidence": sig.get("confidence"),
                "reason": sig.get("reason"),
                "stop_loss": stop_loss,
                "target": target,
                "position_size_pct": position_size_pct,
                "entry_price": price if signal_dir in ("BUY", "SELL") else None,
            })
        return {"status": "ok", "signals": signals}
    except Exception as e:
        _internal_error("Failed to run scan", e)


@app.get("/api/ai-scan")
async def run_ai_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    _auth: None = Depends(_require_api_key),
):
    """Run AI-powered scan on watchlist in the background."""
    _check_rate(request, key="ai-scan:start", limit=RATE_LIMIT_SCAN_START)
    job_id = str(uuid.uuid4())
    jobs.create(job_id, {"status": "pending", "signals": None, "error": None})

    async def _run_scan_job(jid):
        logger.info("  [API] Starting AI scan...")
        start_time = time.time()
        try:
            from ai_strategy import analyze_watchlist_async
            signals = await analyze_watchlist_async()
            duration = time.time() - start_time
            if not signals:
                jobs.update(jid, {
                    "signals": [], "status": "completed",
                    "error": (
                        f"AI scan returned no signals after {duration:.0f}s. "
                        "Check server logs — likely yfinance data fetch failure or AI API issue."
                    ),
                })
                logger.warning(f"  [API] AI scan returned 0 signals in {duration:.1f}s")
            else:
                # Ensure all values are JSON-serializable (no numpy/Decimal types)
                clean = []
                for s in signals:
                    clean.append({
                        k: (float(v) if isinstance(v, (Decimal, float)) and v is not None
                            else str(v) if hasattr(v, '__class__') and v.__class__.__module__ == 'numpy'
                            else v)
                        for k, v in s.items()
                    })
                jobs.update(jid, {"signals": clean, "status": "completed"})
                logger.info(f"  [API] AI scan completed in {duration:.1f}s — {len(clean)} signals")
        except Exception as e:
            duration = time.time() - start_time
            jobs.update(jid, {"error": f"AI scan failed after {duration:.0f}s: {e}", "status": "failed"})
            logger.error(f"  [API] AI scan failed after {duration:.1f}s: {e}")

    background_tasks.add_task(_run_scan_job, job_id)
    return {"status": "accepted", "job_id": job_id}

@app.get("/api/scan/status/{job_id}")
async def get_scan_status(
    job_id: str,
    request: Request,
    _auth: None = Depends(_require_api_key),
):
    """Check the status of a background scan job."""
    _check_rate(request, key="ai-scan:status", limit=RATE_LIMIT_SCAN_STATUS)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return job


@app.post("/api/trade")
def run_trade(
    req: TradeRequest,
    request: Request,
    _auth: None = Depends(_require_api_key),
):
    """Execute one trading cycle."""
    _check_rate(request, key="trade", limit=RATE_LIMIT_TRADE)
    if not _PORTFOLIO_LOCK.acquire(timeout=30):
        raise HTTPException(status_code=503, detail="Another trade is in progress; try again")
    try:
        trader = PaperTrader()
        trader.refresh_portfolio()
        prices = _get_prices_for_trader(trader)
        if not prices:
            raise HTTPException(status_code=500, detail="Could not fetch prices")

        trader.check_stop_loss_take_profit(prices)
        trades_executed = []

        if req.use_ai:
            from ai_strategy import analyze_watchlist
            signals = analyze_watchlist()
            trades_executed = _apply_signals(trader, prices, signals, min_confidence=0.6)
        else:
            for symbol in config.WATCHLIST:
                df = get_historical_data(symbol, period="60d", interval="1d")
                if df.empty:
                    continue
                sig = get_scored_signal(symbol, df)
                trader.refresh_portfolio()
                confidence = sig.get("confidence", 0)
                if confidence < 0.6:
                    continue
                if sig["signal"] == "BUY" and symbol not in trader.portfolio.positions:
                    price = prices.get(symbol, sig["price"])
                    order = trader.buy(symbol, price, confidence=confidence)
                    if order:
                        trades_executed.append({"action": "BUY", "symbol": symbol, "price": price})
                elif sig["signal"] == "SELL" and symbol in trader.portfolio.positions:
                    price = prices.get(symbol, sig["price"])
                    order = trader.sell(symbol, price)
                    if order:
                        trades_executed.append({"action": "SELL", "symbol": symbol, "price": price})

        summary = trader.get_summary(prices)
        return {"status": "ok", "trades": trades_executed, "summary": summary}
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to execute trade cycle", e)
    finally:
        try:
            _PORTFOLIO_LOCK.release()
        except RuntimeError:
            pass


@app.post("/api/order")
def place_manual_order(
    req: OrderRequest,
    request: Request,
    _auth: None = Depends(_require_api_key),
):
    """Execute a manual BUY/SELL from a client UI.

    - `quantity` omitted on BUY → uses Kelly sizing on current cash.
    - `quantity` omitted on SELL → closes the full position.
    - `price` omitted → fetched via live yfinance quote.
    """
    _check_rate(request, key="order", limit=RATE_LIMIT_TRADE)

    portfolio_name = req.portfolio or "main"
    if portfolio_name not in getattr(config, "PORTFOLIOS", {"main": 0}):
        raise HTTPException(status_code=400, detail=f"Unknown portfolio '{portfolio_name}'")

    symbol = req.symbol if req.symbol.endswith(".NS") else req.symbol + ".NS"
    if not _PORTFOLIO_LOCK.acquire(timeout=30):
        raise HTTPException(status_code=503, detail="Another trade is in progress; try again")
    try:
        trader = PaperTrader(name=portfolio_name)
        trader.refresh_portfolio()

        # Resolve fill price.
        price = req.price
        if price is None or price <= 0:
            try:
                from data_fetcher import get_live_price
                live = get_live_price(symbol)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to fetch live price for {symbol}: {e}",
                )
            if live is None or live <= 0:
                raise HTTPException(
                    status_code=502,
                    detail=f"No live price available for {symbol}",
                )
            price = float(live)

        if req.side == "BUY":
            order = trader.buy(symbol, price, quantity=req.quantity)
            if order is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Buy rejected for {symbol} — insufficient funds, non-trading day, "
                           "or position limits exceeded.",
                )
            action = "BUY"
        else:  # SELL
            if symbol not in trader.portfolio.positions:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot sell {symbol} — no open position.",
                )
            order = trader.sell(symbol, price, quantity=req.quantity)
            if order is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Sell rejected for {symbol} — verify quantity vs. holdings.",
                )
            action = "SELL"

        prices = _get_prices_for_trader(trader)
        summary = trader.get_summary(prices)
        return {
            "status": "ok",
            "action": action,
            "symbol": symbol,
            "quantity": order.quantity,
            "price": round(float(order.fill_price()), 2),
            "portfolio": portfolio_name,
            "summary": summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        _internal_error(f"Failed to place {req.side} order for {req.symbol}", e)
    finally:
        try:
            _PORTFOLIO_LOCK.release()
        except RuntimeError:
            pass


@app.post("/api/ai-signals/apply")
def apply_ai_signals(
    req: ApplySignalsRequest,
    request: Request,
    _auth: None = Depends(_require_api_key),
):
    """Apply AI-generated scan signals deterministically without rescanning."""
    _check_rate(request, key="trade", limit=RATE_LIMIT_TRADE)
    if not _PORTFOLIO_LOCK.acquire(timeout=30):
        raise HTTPException(status_code=503, detail="Another trade is in progress; try again")
    try:
        trader = PaperTrader()
        trader.refresh_portfolio()
        prices = _get_prices_for_trader(trader)
        trader.check_stop_loss_take_profit(prices)
        signal_dicts = [s.model_dump() for s in req.signals]
        trades_executed = _apply_signals(trader, prices, signal_dicts, min_confidence=float(req.min_confidence))
        summary = trader.get_summary(prices)
        return {"status": "ok", "trades": trades_executed, "summary": summary}
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to apply AI signals", e)
    finally:
        try:
            _PORTFOLIO_LOCK.release()
        except RuntimeError:
            pass


@app.post("/api/autopilot/start")
def start_autopilot(
    req: AutopilotRequest,
    _auth: None = Depends(_require_api_key),
):
    """Start the autopilot systemd service."""
    status = _get_service_status()
    if status["running"]:
        raise HTTPException(status_code=400, detail="Autopilot already running")

    try:
        result = _systemctl("start")
        if result.returncode != 0:
            logger.error(f"Failed to start autopilot service: {result.stderr.strip()}")
            raise HTTPException(status_code=500, detail="Failed to start autopilot service")
        return {"status": "ok", "message": "Autopilot service started"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Timed out starting service")
    except Exception as e:
        _internal_error("Failed to start autopilot", e)


@app.post("/api/autopilot/stop")
def stop_autopilot(_auth: None = Depends(_require_api_key)):
    """Stop the autopilot systemd service."""
    status = _get_service_status()
    if not status["running"]:
        raise HTTPException(status_code=400, detail="Autopilot not running")

    try:
        result = _systemctl("stop")
        if result.returncode != 0:
            logger.error(f"Failed to stop autopilot service: {result.stderr.strip()}")
            raise HTTPException(status_code=500, detail="Failed to stop autopilot service")
        return {"status": "ok", "message": "Autopilot service stopped"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Timed out stopping service")
    except Exception as e:
        _internal_error("Failed to stop autopilot", e)


@app.post("/api/watchlist/scan")
def force_watchlist_scan(_auth: None = Depends(_require_api_key)):
    """Force-run the trending stock scanner so the watchlist refreshes
    immediately instead of waiting for the autopilot's 45-min cadence.

    The autopilot loop owns the canonical scan_trending_stocks() call and
    persists its state via SQLite, so this endpoint just invokes it with
    the current cycle and held symbols. The autopilot picks up the
    refreshed watchlist on its next cycle (it reads config.WATCHLIST,
    which scan_trending_stocks mutates in place)."""
    try:
        from autopilot import scan_trending_stocks, CYCLE_COUNT_FILE, WATCHLIST_STATE_FILE
        from persistence import read_json
    except Exception as e:
        _internal_error("Failed to import autopilot module", e)

    cycle = 0
    try:
        with open(CYCLE_COUNT_FILE) as cf:
            cycle = int(cf.read().strip())
    except (OSError, ValueError):
        pass

    state = read_json(WATCHLIST_STATE_FILE, default={})
    stale_counts = state.get("stale_counts") or {}

    held: set[str] = set()
    try:
        for name in getattr(config, "PORTFOLIOS", {"main": 0}).keys():
            t = PaperTrader(name=name)
            held.update(t.portfolio.positions.keys())
    except Exception:
        pass

    try:
        added = scan_trending_stocks(
            held_symbols=held,
            cycle_num=cycle,
            stale_counts=stale_counts,
        )
    except Exception as e:
        _internal_error("Watchlist scan failed", e)

    refreshed = read_json(WATCHLIST_STATE_FILE, default={})
    return {
        "status": "ok",
        "added": added,
        "watchlist": refreshed.get("watchlist", []),
        "stale_counts": refreshed.get("stale_counts", {}),
        "updated_at": refreshed.get("updated_at"),
    }


@app.get("/api/watchlist")
def get_watchlist():
    """Return the current watchlist plus per-symbol staleness, so the app
    can show which symbols are quiet and prompt the user to rescan."""
    try:
        from autopilot import WATCHLIST_STATE_FILE
        from persistence import read_json
    except Exception as e:
        _internal_error("Failed to import autopilot module", e)

    state = read_json(WATCHLIST_STATE_FILE, default={})
    return {
        "status": "ok",
        "watchlist": state.get("watchlist", list(getattr(config, "WATCHLIST", []))),
        "stale_counts": state.get("stale_counts", {}),
        "hold_cycles": state.get("hold_cycles", {}),
        "updated_at": state.get("updated_at"),
    }


@app.get("/api/journal")
def get_journal(portfolio: str = "main"):
    """Get trade journal entries. `?portfolio=main|eval`."""
    if portfolio not in getattr(config, "PORTFOLIOS", {"main": 0}):
        raise HTTPException(status_code=400, detail=f"Unknown portfolio '{portfolio}'")
    try:
        from learner import _journal_path
        from persistence import read_json
        journal = read_json(_journal_path(portfolio), default=[]) or []
        return {"status": "ok", "portfolio": portfolio, "entries": journal[-50:]}
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to get journal", e)


@app.get("/api/lessons")
def get_lessons(portfolio: str = "main"):
    """Get lessons learned. `?portfolio=main|eval`."""
    if portfolio not in getattr(config, "PORTFOLIOS", {"main": 0}):
        raise HTTPException(status_code=400, detail=f"Unknown portfolio '{portfolio}'")
    try:
        from learner import _lessons_path
        from persistence import read_json
        lessons = read_json(_lessons_path(portfolio), default=[]) or []
        return {"status": "ok", "portfolio": portfolio, "lessons": lessons}
    except HTTPException:
        raise
    except Exception as e:
        _internal_error("Failed to get lessons", e)


@app.get("/api/logs/dates")
def get_log_dates():
    """List available log dates (from rotated log files)."""
    try:
        log_dir = os.path.dirname(LOG_FILE)
        today = datetime.now().strftime("%Y-%m-%d")
        dates = [today]  # Current active log always represents today

        if os.path.isdir(log_dir):
            for f in os.listdir(log_dir):
                # Rotated files: trading_agent.log.2026-04-09
                if f.startswith("trading_agent.log.") and len(f.split(".")[-1]) == 10:
                    date_part = f.split(".")[-1]
                    if date_part not in dates:
                        dates.append(date_part)

        dates.sort(reverse=True)
        return {"status": "ok", "dates": dates}
    except Exception as e:
        _internal_error("Failed to list log dates", e)


@app.get("/api/logs/recent")
def get_recent_logs(lines: int = 500, date: str | None = None):
    """Get log lines, optionally filtered by date (YYYY-MM-DD). Defaults to today."""
    lines = min(max(lines, 1), 5000)  # Cap to prevent DoS
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        target_date = date or today

        # Determine which file to read
        if target_date == today:
            log_path = LOG_FILE  # Active log file
        else:
            log_path = f"{LOG_FILE}.{target_date}"  # Rotated file

        if not os.path.exists(log_path):
            return {"status": "ok", "logs": [], "date": target_date}

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        # For the active log file, filter to only lines matching target_date
        if log_path == LOG_FILE:
            filtered = [l.strip() for l in all_lines if l.strip() and l.startswith(target_date)]
            # If no timestamped lines match (e.g. plain output), include all
            if not filtered:
                filtered = [l.strip() for l in all_lines if l.strip()]
        else:
            filtered = [l.strip() for l in all_lines if l.strip()]

        recent = filtered[-lines:]
        return {"status": "ok", "logs": recent, "date": target_date}
    except Exception as e:
        _internal_error("Failed to get recent logs", e)


@app.get("/api/training-log")
def get_training_log():
    """Get ML training history."""
    try:
        path = os.path.join(config.PROJECT_DIR, "training_log.json")
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            return {"status": "ok", "training_log": data[-20:]}
        return {"status": "ok", "training_log": []}
    except Exception as e:
        _internal_error("Failed to get training log", e)


@app.post("/api/chat")
async def chat_with_agent(
    req: ChatRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    _auth: None = Depends(_require_api_key),
):
    """Chat with the AI trading agent in background."""
    _check_rate(request, key="chat:start", limit=RATE_LIMIT_CHAT_START)
    job_id = str(uuid.uuid4())
    jobs.create(job_id, {"status": "pending", "reply": None, "action": None, "error": None})

    async def _run_chat_job(jid, message, history):
        try:
            async def _to_thread_with_timeout(func, *args, timeout: float = 10.0, default=None):
                try:
                    return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=timeout)
                except Exception:
                    return default

            try:
                from learner import get_learning_context, get_performance_stats
            except Exception:
                # Keep chat usable even if learner module import fails in a partial deploy.
                def get_learning_context():
                    return ""

                def get_performance_stats():
                    return {}

            # Check for buy/sell commands first
            trader = PaperTrader()
            cmd_result = await _to_thread_with_timeout(_execute_chat_command, message, trader, timeout=8.0, default=None)
            if cmd_result:
                jobs.update(jid, {"reply": cmd_result, "action": "command", "status": "completed"})
                return

            prices = await _to_thread_with_timeout(_get_prices_for_trader, trader, timeout=8.0, default={}) or {}
            summary = await _to_thread_with_timeout(
                trader.get_summary,
                prices,
                timeout=8.0,
                default={
                    "cash": 0.0,
                    "total_value": 0.0,
                    "total_return_pct": 0.0,
                    "realized_pnl": 0.0,
                },
            )
            portfolio_text = _get_chat_portfolio_text(trader, prices, summary)
            system_prompt = _build_chat_system_prompt()

            market_data = await _to_thread_with_timeout(
                _get_chat_market_snapshot,
                timeout=8.0,
                default="Market data unavailable.",
            )

            learning_data = await _to_thread_with_timeout(
                get_learning_context,
                timeout=6.0,
                default="",
            )

            stats = await _to_thread_with_timeout(
                get_performance_stats,
                timeout=6.0,
                default={},
            ) or {}
            stats_text = ""
            if stats.get("total_trades", 0) > 0:
                stats_text = (
                    f"\nPERFORMANCE: Win Rate {stats.get('win_rate', 0):.0f}%, "
                    f"Avg Win Rs.{stats.get('avg_win_pct', 0):.2f}%, "
                    f"Avg Loss Rs.{stats.get('avg_loss_pct', 0):.2f}%"
                )

            convo_text = ""
            if history:
                for msg in history[-8:]:
                    prefix = "U" if msg.role == "user" else "A"
                    convo_text += f"{prefix}:{msg.content[:200]}\n"

            try:
                from news_sentiment import fetch_all_news, format_news_for_ai
                news_data = await _to_thread_with_timeout(fetch_all_news, timeout=12.0, default=None)
                news_text = format_news_for_ai(news_data) if news_data is not None else "News data unavailable."
            except Exception:
                news_text = "News data unavailable."

            ai_prompt = f"""{system_prompt}

PORTFOLIO DATA:
{portfolio_text}
{stats_text}

MARKET DATA:
{market_data}

{news_text}

TRADE HISTORY:
{learning_data}

CONVERSATION:
{convo_text}

User: {message}

Respond helpfully and concisely:"""

            reply: str | Any | None = None
            # Copilot/Haiku -> Ollama -> OpenRouter -> Groq -> Cloudflare -> Gemini
            _chat_providers = [
                ("Copilot", _chat_call_copilot, 30.0),
                ("Ollama", _chat_call_ollama, 240.0),
                ("OpenRouter", _chat_call_openrouter, 30.0),
                ("Groq", _chat_call_groq, 15.0),
                ("Cloudflare", _chat_call_cloudflare, 30.0),
                ("Gemini", _chat_call_gemini, 30.0),
            ]
            for _name, _fn, _timeout in _chat_providers:
                if not _provider_available(_name):
                    logger.info(f"  [API] Skipping {_name} (cooling down)")
                    continue
                try:
                    reply = await asyncio.wait_for(asyncio.to_thread(_fn, ai_prompt), timeout=_timeout)
                    _mark_provider_ok(_name)
                    break
                except Exception as e:
                    logger.warning(f"  [API] {_name} chat failed: {e}")
                    _mark_provider_failed(_name, e)
                    continue

            if reply is None:
                reply = "AI response is taking longer than expected right now. Please try again in a moment."

            if not isinstance(reply, str):
                reply = str(reply)

            jobs.update(jid, {"reply": reply, "action": "chat", "status": "completed"})

        except Exception as e:
            jobs.update(jid, {"status": "failed", "error": "Chat processing failed"})
            logger.error(f"  [API] Chat job failed: {e}")

    background_tasks.add_task(_run_chat_job, job_id, req.message, req.history)
    return {"status": "accepted", "job_id": job_id}

@app.get("/api/chat/status/{job_id}")
async def get_chat_status(
    job_id: str,
    request: Request,
    _auth: None = Depends(_require_api_key),
):
    """Check the status of a background chat job."""
    _check_rate(request, key="chat:status", limit=RATE_LIMIT_CHAT_STATUS)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return job


# ── WebSocket for live log streaming ──

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    """Stream log file updates in real-time."""
    token = websocket.headers.get("x-api-key") or websocket.query_params.get("token")
    ok, _status, _msg = _is_api_key_valid(token)
    if not ok:
        await websocket.close(code=1008)
        return
    await log_broadcaster.connect(websocket)
    try:
        while True:
            # Keep connection alive, receive any client messages (ping/pong)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        log_broadcaster.disconnect(websocket)


# ── Forex Module Loader ──

def _load_forex_modules():
    """Load forex agent modules under 'fx_' prefix to avoid name collisions."""
    import importlib.util

    forex_dir = os.environ.get("FOREX_AGENT_DIR", "")
    if not forex_dir:
        candidates = [
            Path(__file__).resolve().parent.parent / "forex-trading-agent",
            Path(__file__).resolve().parent.parent.parent / "forex-trading-agent",
        ]
        for c in candidates:
            if c.exists():
                forex_dir = str(c)
                break
    if not forex_dir or not os.path.exists(forex_dir):
        return None

    loaded = {}
    for mod_name in ["config", "data_fetcher", "paper_trader", "market_calendar", "strategy"]:
        mod_path = os.path.join(forex_dir, f"{mod_name}.py")
        if not os.path.exists(mod_path):
            continue
        spec = importlib.util.spec_from_file_location(f"fx_{mod_name}", mod_path)
        mod = importlib.util.module_from_spec(spec)
        # Inject forex config into sub-modules
        if mod_name != "config" and "config" in loaded:
            sys.modules["config"] = loaded["config"]
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            logger.warning(f"Failed to load forex module {mod_name}: {e}")
            continue
        finally:
            # Restore NSE config
            import config as _nse_config
            sys.modules["config"] = _nse_config
        loaded[mod_name] = mod
    return loaded if "config" in loaded else None


_fx = _load_forex_modules()


# ── Forex API Endpoints ──

if _fx:
    from fastapi import APIRouter
    forex_router = APIRouter(prefix="/api/forex", tags=["forex"])

    @forex_router.get("/status")
    async def forex_status(x_api_key: str | None = Header(default=None, alias="X-API-Key")):
        _require_api_key(x_api_key)
        fx_config = _fx["config"]
        fx_pt = _fx["paper_trader"]
        fx_df = _fx["data_fetcher"]
        fx_cal = _fx["market_calendar"]

        trader = fx_pt.PaperTrader(filepath=os.path.join(fx_config.PROJECT_DIR, "portfolio.json"))
        prices = {}
        try:
            prices = fx_df.get_watchlist_prices()
        except Exception:
            pass
        summary = trader.get_summary(prices)

        sessions = []
        try:
            sessions = fx_cal.get_active_sessions()
        except Exception:
            pass

        return {
            "summary": summary,
            "positions": {
                sym: {
                    "symbol": sym,
                    "quantity": pos.quantity,
                    "avg_price": float(pos.avg_price),
                    "current_price": prices.get(sym, float(pos.avg_price)),
                    "pnl": float(pos.pnl(fx_pt.D(prices.get(sym, float(pos.avg_price))))),
                    "pnl_pct": float(pos.pnl_pct(fx_pt.D(prices.get(sym, float(pos.avg_price))))),
                }
                for sym, pos in trader.portfolio.positions.items()
            },
            "trades": trader.portfolio.trade_log[-20:],
            "market": {
                "open": fx_cal.is_market_open(),
                "sessions": sessions,
            },
            "autopilot": _get_forex_autopilot_status(fx_config),
            "currency": "$",
        }

    def _get_forex_autopilot_status(fx_config):
        try:
            import subprocess
            result = subprocess.run(
                ["sudo", "systemctl", "show", "forex-trading-agent.service",
                 "--property=ActiveState,SubState,MainPID,ExecMainStartTimestamp"],
                capture_output=True, text=True, timeout=5,
            )
            props = dict(line.split("=", 1) for line in result.stdout.strip().splitlines() if "=" in line)
            running = props.get("ActiveState") == "active" and props.get("SubState") == "running"
            cycle = 0
            cycle_file = os.path.join(fx_config.PROJECT_DIR, "logs", "cycle_count.txt")
            if running:
                try:
                    with open(cycle_file, "r") as f:
                        cycle = int(f.read().strip())
                except Exception:
                    pass
            return {
                "running": running,
                "cycle": cycle,
                "started_at": props.get("ExecMainStartTimestamp", "") if running else None,
                "interval": 15,
                "pid": int(props.get("MainPID", 0)),
            }
        except Exception:
            return {"running": False, "cycle": 0, "started_at": None, "interval": 15, "pid": 0}

    @forex_router.get("/prices")
    async def forex_prices(x_api_key: str | None = Header(default=None, alias="X-API-Key")):
        _require_api_key(x_api_key)
        fx_df = _fx["data_fetcher"]
        prices = fx_df.get_watchlist_prices()
        return {"prices": prices, "currency": "$"}

    @forex_router.get("/market-regime")
    async def forex_regime(x_api_key: str | None = Header(default=None, alias="X-API-Key")):
        _require_api_key(x_api_key)
        fx_df = _fx["data_fetcher"]
        regime = fx_df.get_market_regime()
        return {"regime": regime, "index": _fx["config"].MARKET_INDEX}

    @forex_router.get("/candles")
    async def forex_candles(
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ):
        _require_api_key(x_api_key)
        fx_df = _fx["data_fetcher"]
        period_map = {"1m": "1d", "5m": "5d", "15m": "30d", "1h": "60d", "4h": "60d", "1d": "1y"}
        period = period_map.get(timeframe, "60d")
        df = fx_df.get_historical_data(symbol, period=period, interval=timeframe)
        if df.empty:
            return {"candles": [], "symbol": symbol}
        df = df.tail(limit)
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "time": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                "open": round(float(row["Open"]), 5),
                "high": round(float(row["High"]), 5),
                "low": round(float(row["Low"]), 5),
                "close": round(float(row["Close"]), 5),
                "volume": int(row.get("Volume", 0)),
            })
        return {"candles": candles, "symbol": symbol, "currency": "$"}

    app.include_router(forex_router)
    logger.info("  [FOREX] Forex API endpoints loaded at /api/forex/")
else:
    logger.info("  [FOREX] Forex agent not found, /api/forex/ endpoints disabled")


# ── Entry point ──

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("API_PORT", "8000"))
    # Use app-specific env names to avoid overriding global SSL trust-store vars
    # used by outbound HTTPS clients (e.g., Gemini/Yahoo requests).
    ssl_certfile = os.environ.get("API_SSL_CERT_FILE") or os.environ.get("SSL_CERT_FILE")
    ssl_keyfile = os.environ.get("API_SSL_KEY_FILE") or os.environ.get("SSL_KEY_FILE")
    kwargs = {"host": "0.0.0.0", "port": port}
    if ssl_certfile and ssl_keyfile:
        kwargs["ssl_certfile"] = ssl_certfile
        kwargs["ssl_keyfile"] = ssl_keyfile
    uvicorn.run(app, **kwargs)
