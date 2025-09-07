Headless Deployment (24/7)

Two simple options:

1) Docker on a VPS (recommended)
   - Works on any small Linux VM (DigitalOcean/Linode/Lightsail/etc.).
   - Auto‑restart on failure/boot via Docker restart policy.

2) Systemd (no Docker)
   - Install Python 3.10+ on the server and run as a service.

Below is the Docker path, which is the least moving parts.

Prereqs (Ubuntu 22.04+)
- Install Docker Engine + Compose plugin:
  - sudo apt-get update
  - sudo apt-get install -y ca-certificates curl gnupg
  - sudo install -m 0755 -d /etc/apt/keyrings
  - curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  - echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  - sudo apt-get update && sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  - sudo usermod -aG docker $USER  # then log out/in to use docker without sudo

Clone and configure
- git clone <your-repo> perp-bot && cd perp-bot
- Copy your env files (never commit real keys):
  - cp smart_scanner/.env smart_scanner/.env.local  # then edit .env.local for your server
- Set runtime toggles in smart_scanner/.env.local (examples):
  - ENABLE_AUTOTRADE=0          # 1 to enable live routing
  - PAPER_TRADING=1             # keep 1 while testing; set 0 for live
  - METRICS_ENABLED=1
  - METRICS_PATH=/data/scanner_metrics.jsonl
  - BANDIT_STATE_PATH=/data/bandit_state.json
  - UNIVERSE_CACHE_PATH=/data/.universe_cache.json

Build and run (local build)
- docker compose up -d --build
- docker compose logs -f    # tail logs

Update
- git pull && docker compose up -d --build

Stop and remove
- docker compose down

Data persistence
- Metrics, bandit state, and caches are kept in a named volume `perpdata` mounted at /data inside the container.

Troubleshooting
- If logs show rate limits, consider increasing TF sleeps or reducing TOP_SYMBOLS in your env.
- If nothing prints, ensure your env files are being loaded: `docker compose exec perp-bot env | grep -E "ENABLE_AUTOTRADE|PAPER_TRADING|METRICS_PATH"`.

Systemd (no Docker)
1) Install Python 3.10+ and pip on the server.
2) Create a venv and install the package:
   - python3 -m venv .venv && source .venv/bin/activate
   - pip install --upgrade pip
   - pip install -e smart_scanner
3) Create /etc/systemd/system/perp-bot.service with:
   [Unit]
   Description=Perp Bot Scanner
   After=network-online.target

   [Service]
   WorkingDirectory=%h/perp-bot
   Environment=PYTHONUNBUFFERED=1
   Environment=ENV_FILE=%h/perp-bot/smart_scanner/.env
   ExecStart=%h/perp-bot/.venv/bin/python -m smart_scanner.scanner_runner --loop
   Restart=always
   RestartSec=5
   StandardOutput=journal
   StandardError=journal

   [Install]
   WantedBy=multi-user.target

4) Enable and start:
   - systemctl --user daemon-reload
   - systemctl --user enable --now perp-bot.service
- journalctl --user -u perp-bot -f


CI/CD: Build + Push Image to GHCR
----------------------------------
This repo includes a GitHub Actions workflow that builds and publishes your image to GitHub Container Registry (GHCR) on every push to main and on tags.

Steps:
1) Push the repo to GitHub (or ensure it already lives there).
2) The workflow at `.github/workflows/docker-image.yml` uses the built-in `GITHUB_TOKEN` to push to `ghcr.io/<owner>/<repo>`.
   - First run will create a private package under your account/org in GHCR.
   - Option A (public image): in the GitHub UI, open the package page and set visibility to Public.
   - Option B (keep private): create a Personal Access Token (classic) with `read:packages` (and `write:packages` for publishing from other repos), then login on servers that pull.
3) Verify the action ran and produced tags (latest, branch, sha, etc.).

Deploy with the prebuilt image
1) Edit `compose.prod.yaml` and replace `ghcr.io/OWNER/REPO:latest` with your actual GHCR path (lowercase), for example:
   - `ghcr.io/youruser/perp_bot:latest` or `ghcr.io/yourorg/perp-bot:latest`
2) On the server:
   - If image is public: just pull and run.
     - docker compose -f compose.prod.yaml pull
     - docker compose -f compose.prod.yaml up -d
   - If image is private: authenticate once and then pull.
     - echo <YOUR_PAT> | docker login ghcr.io -u <YOUR_GH_USER> --password-stdin
     - docker compose -f compose.prod.yaml pull
     - docker compose -f compose.prod.yaml up -d
3) Update to a new version: re-run pull+up after your CI publishes a fresh image.


Reverse Proxy (Caddy) with TLS + Basic Auth
-------------------------------------------
This repo includes a small Caddy proxy that fronts the dashboard over HTTPS with Basic Auth.

Prereqs
- DNS A record pointing to your server (e.g., dashboard.example.com).
- Open ports 80 and 443 in your cloud firewall.

Configure
1) Generate a bcrypt password hash (replace 'YOURPASS'):
   - docker run --rm caddy caddy hash-password --plaintext 'YOURPASS'
2) Set environment variables when running compose (or export them in your shell):
   - DASHBOARD_DOMAIN=dashboard.example.com
   - DASHBOARD_EMAIL=you@example.com
   - DASHBOARD_ADMIN_USER=admin
   - DASHBOARD_ADMIN_PASSHASH=<paste bcrypt hash>
3) Start proxy + dashboard:
   - DASHBOARD_DOMAIN=dashboard.example.com \
     DASHBOARD_EMAIL=you@example.com \
     DASHBOARD_ADMIN_USER=admin \
     DASHBOARD_ADMIN_PASSHASH='<bcrypt>' \
     docker compose -f compose.prod.yaml up -d --build proxy dashboard

Access
- https://dashboard.example.com (use the Basic Auth credentials you set)
- The Caddyfile is at `Caddyfile`. Adjust headers or add routes as needed.

Notes
- Keep the scanner container private; the proxy only exposes the dashboard.
- Caddy stores TLS state in the named volumes `caddy_data` and `caddy_config`.
