# GPU Dashboard

A web dashboard that summarizes GPU utilization across SSH-accessible nodes by running `nvidia-smi` on each host.

## Setup

### 1. Requirements

- The machine running the dashboard must be able to SSH directly to each node
- `~/.ssh/id_ed25519_hakone` must exist (or edit `src/config.py` to change `SSH_IDENTITY_FILE`)
- SSH options (User, IdentityFile) are passed explicitly; `~/.ssh/config` is optional

### 2. Install and run

```bash
cd baulab-dashboard  # after: git clone git@github.com:wendlerc/baulab-dashboard.git
uv sync
uv run python -m src.main
```

Open http://localhost:5000

### 3. Expose via Cloudflare Tunnel (optional)

To access from outside your network:

```bash
# Terminal 1: start the dashboard
uv run python -m src.main

# Terminal 2: start the tunnel
./run-tunnel.sh
```

The tunnel prints a URL like `https://random-name.trycloudflare.com` — open it in a browser. No Cloudflare account required.

### 4. Always-on (systemd, survives reboot)

To run the dashboard automatically on boot with Flask + cloudflared + auto-push URL to GitHub:

**1. Configure the env file**

```bash
mkdir -p ~/.config/baulab-dashboard
nano ~/.config/baulab-dashboard/env
```

**Option A: Cloudflare named tunnel (recommended)** — no rate limits, stable URL:
```
CLOUDFLARE_TUNNEL_TOKEN=eyJ...
CLOUDFLARE_TUNNEL_URL=https://your-tunnel.example.com
BAULAB_SSH_PASSPHRASE_FILE=/path/to/.config/baulab-dashboard/ssh-passphrase
```

**Option B: Quick tunnel** — requires SSH passphrase for git push:
```
BAULAB_SSH_PASSPHRASE_FILE=/path/to/.config/baulab-dashboard/ssh-passphrase
```

**2. Install the systemd user service**

```bash
cd /path/to/gpu-dashboard  # or baulab-dashboard

mkdir -p ~/.config/systemd/user
cp baulab-dashboard.service ~/.config/systemd/user/

# Edit the service file: update WorkingDirectory, ExecStart, and EnvironmentFile
# to match your paths (e.g. ~/code/baulab-dashboard)
nano ~/.config/systemd/user/baulab-dashboard.service

systemctl --user daemon-reload
systemctl --user enable baulab-dashboard
systemctl --user start baulab-dashboard
```

**3. Enable user services at boot (without login)**

```bash
loginctl enable-linger
```

**4. Verify**

```bash
systemctl --user status baulab-dashboard
journalctl --user -u baulab-dashboard -f
```

Startup logs are also written to `/tmp/baulab-dashboard-startup.log`.

## How it works

- A background thread runs `nvidia-smi` on each node every 60 seconds via SSH
- The web UI polls `/api/status` every 30 seconds
- Nodes are queried in parallel; connection timeout is 15 seconds per node
