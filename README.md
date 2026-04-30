# nse-backend

FastAPI backend that wraps the [`nse-agent`](https://github.com/sppidy/nse-agent) and exposes it over HTTP + WebSocket. Part of [`trading-agent`](https://github.com/sppidy/trading-agent).

Single-user, in-memory job store (TTL 600s). Designed for personal deployment behind a private network or VPN.

## Quick start

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                                      # set API_AUTH_TOKEN at minimum
export AGENT_DIR=../nse-agent                              # path to the agent submodule
./start_server.sh                                          # or start_server.bat on Windows
```

Server listens on `${API_PORT}` (default 8443) and serves the bundled web dashboard at `/dashboard` (mount points at `../web/`).

## Auth

All `/api/*` endpoints require `X-API-Key: <API_AUTH_TOKEN>` header. WebSocket `/ws/logs` accepts the token via header **or** `?token=` query param.

For local dev only, set `ALLOW_INSECURE_API_TOKEN=1` and skip `API_AUTH_TOKEN`. Never do this in production.

## Endpoints (prefix `/api/`)

| Group | Routes |
| --- | --- |
| Read    | `status`, `prices`, `candles`, `market-regime`, `watchlist`, `journal`, `lessons`, `logs/*`, `training-log` |
| Scan    | `scan`, `ai-scan`, `scan/status/{job_id}` |
| Trade   | `trade`, `ai-signals/apply`, `order` |
| Autopilot | `autopilot/start`, `autopilot/stop` |
| Chat    | `chat`, `chat/status/{job_id}` |
| Stream  | `/ws/logs` (WebSocket) |

## Deployment

`ai-trader-api.service` is a sample systemd unit. Adjust `WorkingDirectory`, `AGENT_DIR`, and certificate paths to your environment. See [`docs/DEPLOY_SECURITY_NOTES.md`](https://github.com/sppidy/trading-agent/blob/main/docs/DEPLOY_SECURITY_NOTES.md) in the super repo for the hardening checklist (auth token, sudoers allowlist, self-signed TLS).

## License

[Apache-2.0](LICENSE). Contributing guidelines and security policy live in the [super-repo](https://github.com/sppidy/trading-agent).
