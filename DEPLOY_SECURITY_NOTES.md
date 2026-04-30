## Backend security notes

### 1) Protected API token

Set `API_AUTH_TOKEN` on the server (must match Android `apiKey`):

```bash
sudo systemctl edit ai-trader-api.service
```

Add:

```ini
[Service]
Environment=API_AUTH_TOKEN=replace-with-strong-random-token
Environment=TRUSTED_ORIGINS=http://${BACKEND_HOST}:8000,https://${BACKEND_HOST}:8443
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl restart ai-trader-api.service
```

### 2) Restrict sudo access for systemctl

Use a strict sudoers allowlist so backend user can only control the target service:

```bash
sudo visudo -f /etc/sudoers.d/ai-trader-api
```

Add:

```text
ubuntu ALL=(root) NOPASSWD: /usr/bin/systemctl start ai-trading-agent.service
ubuntu ALL=(root) NOPASSWD: /usr/bin/systemctl stop ai-trading-agent.service
ubuntu ALL=(root) NOPASSWD: /usr/bin/systemctl show ai-trading-agent.service --property=ActiveState,SubState,MainPID,ExecMainStartTimestamp
```

Do not grant wildcard sudo access.

### 3) Enable self-signed TLS on 8443

Create cert/key on server:

```bash
mkdir -p /home/ubuntu/backend/certs
openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout /home/ubuntu/backend/certs/api.key \
  -out /home/ubuntu/backend/certs/api.crt \
  -days 825 \
  -subj "/CN=${BACKEND_HOST}" \
  -addext "subjectAltName = IP:${BACKEND_HOST}"
chmod 600 /home/ubuntu/backend/certs/api.key
```

Set service env:

```ini
[Service]
Environment=API_PORT=8443
Environment=API_SSL_CERT_FILE=/home/ubuntu/backend/certs/api.crt
Environment=API_SSL_KEY_FILE=/home/ubuntu/backend/certs/api.key
UnsetEnvironment=SSL_CERT_FILE SSL_KEY_FILE
```

Restart service:

```bash
sudo systemctl daemon-reload
sudo systemctl restart ai-trader-api.service
```

Export `/home/ubuntu/backend/certs/api.crt` and bundle it as Android `@raw/backend_cert` for app trust.
