#!/bin/bash
# Orchestrates: Flask → cloudflared → capture URL → push to GitHub
# Run as systemd service for always-on after reboot.

set -e
cd "$(dirname "$0")"

CLOUDFLARED="${CLOUDFLARED:-${HOME}/.local/bin/cloudflared}"
if [[ ! -x "$CLOUDFLARED" ]]; then
  CLOUDFLARED="cloudflared"
fi
echo "[$(date)] Using cloudflared: $CLOUDFLARED"

# SSH key for git push (GitHub). If it has a passphrase, set one of:
#   BAULAB_SSH_PASSPHRASE_FILE  - path to file containing passphrase (chmod 600)
#   BAULAB_SSH_PASSPHRASE       - passphrase directly (less secure)
GIT_SSH_KEY="${GIT_SSH_KEY:-$HOME/.ssh/id_ed25519}"

LOG_FILE="${LOG_FILE:-/tmp/baulab-dashboard-startup.log}"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date)] Starting GPU dashboard orchestration..."

# Free port 5000 - from previous run or crashed instance
fuser -k 5000/tcp 2>/dev/null || true
sleep 1

# Load SSH key for git push (needed when running at boot without agent)
# Set BAULAB_SSH_PASSPHRASE_FILE (path to file with passphrase) or BAULAB_SSH_PASSPHRASE
_load_ssh_key() {
  local askpass pf
  if [[ -n "$BAULAB_SSH_PASSPHRASE_FILE" && -f "$BAULAB_SSH_PASSPHRASE_FILE" ]]; then
    pf="$BAULAB_SSH_PASSPHRASE_FILE"
  elif [[ -n "$BAULAB_SSH_PASSPHRASE" ]]; then
    pf=$(mktemp)
    printf '%s' "$BAULAB_SSH_PASSPHRASE" > "$pf"
    chmod 600 "$pf"
  else
    return 1
  fi
  askpass=$(mktemp)
  printf '#!/bin/sh\ncat "%s"\n' "$pf" > "$askpass"
  chmod 700 "$askpass"
  eval "$(ssh-agent -s)"
  DISPLAY=:0 SSH_ASKPASS="$askpass" SSH_ASKPASS_REQUIRE=force ssh-add "$GIT_SSH_KEY" 2>/dev/null
  rm -f "$askpass"
  [[ "$pf" != "$BAULAB_SSH_PASSPHRASE_FILE" ]] && rm -f "$pf"
}
if [[ -f "$GIT_SSH_KEY" ]]; then
  _load_ssh_key || true
fi

# 1. Start Flask in background (cloudflared needs something to tunnel to)
# Ensure uv and cloudflared are in PATH (systemd has minimal env)
export PATH="${HOME}/.local/bin:/usr/local/bin:/usr/bin:$PATH"
echo "[$(date)] Starting Flask server..."
if command -v uv &>/dev/null; then
  uv run python -m src.main &
else
  .venv/bin/python -m src.main &
fi
FLASK_PID=$!
sleep 3  # Give Flask time to bind to port 5000

# 2. Start cloudflared
URL=""
if [[ -n "$CLOUDFLARE_TUNNEL_TOKEN" ]]; then
  # Named tunnel - stable URL, no rate limits
  echo "[$(date)] Using Cloudflare named tunnel"
  "$CLOUDFLARED" tunnel run --token "$CLOUDFLARE_TUNNEL_TOKEN" &
  URL="${CLOUDFLARE_TUNNEL_URL:-}"
else
  # Quick tunnel - capture URL, may hit rate limits
  for attempt in 1 2 3; do
    CF_LOG=$(mktemp)
    if command -v stdbuf &>/dev/null; then
      stdbuf -oL -eL "$CLOUDFLARED" tunnel --url http://localhost:5000 > "$CF_LOG" 2>&1 &
    else
      "$CLOUDFLARED" tunnel --url http://localhost:5000 > "$CF_LOG" 2>&1 &
    fi
    CF_PID=$!

    echo "[$(date)] Waiting for cloudflared tunnel URL - attempt $attempt of 3..."
    for i in $(seq 1 90); do
      URL=$(grep -oE 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$CF_LOG" 2>/dev/null | head -1)
      if [[ -n "$URL" ]]; then
        rm -f "$CF_LOG"
        break 2
      fi
      if grep -q "429 Too Many Requests" "$CF_LOG" 2>/dev/null; then
        echo "[$(date)] Cloudflare rate limit 429, will retry after backoff"
        kill $CF_PID 2>/dev/null || true
        rm -f "$CF_LOG"
        break
      fi
      sleep 1
    done

    if [[ -z "$URL" ]]; then
      kill $CF_PID 2>/dev/null || true
      if [[ $attempt -lt 3 ]]; then
        echo "[$(date)] Retry in 120s..."
        sleep 120
      else
        echo "[$(date)] ERROR: Failed to get cloudflared URL after 3 attempts"
        cat "$CF_LOG" 2>/dev/null | head -50
        rm -f "$CF_LOG"
        echo "[$(date)] Continuing without tunnel - dashboard at http://localhost:5000"
        break
      fi
      rm -f "$CF_LOG"
    fi
  done
fi

[[ -n "$URL" ]] && echo "[$(date)] Got tunnel URL: $URL"

# 3. Update index.html and push to GitHub (only if we have a URL)
if [[ -n "$URL" ]]; then
  sed -i "s|window.GPU_DASHBOARD_API = \"https://[^\"]*\";|window.GPU_DASHBOARD_API = \"$URL\";|" public/index.html
  if ! git diff --quiet public/index.html; then
    git add public/index.html
    git commit -m "Update tunnel URL: $URL"
    git pull --rebase origin main 2>/dev/null || true
    if git push origin main; then
      echo "[$(date)] Pushed new URL to GitHub"
    else
      echo "[$(date)] WARNING: git push failed"
    fi
  fi
fi

# 5. Wait for Flask (keeps service alive; cloudflared stays as sibling)
wait $FLASK_PID
