#!/usr/bin/env bash
# Deploy local working-tree changes to the Foundry VM.
# File list is derived dynamically from `git status --porcelain`:
# modified (M), added (A), and untracked (??) files vs HEAD. Deletions
# and renames-source are skipped; for renames we ship the new path.
# Gitignored files (config.toml, data/, __pycache__) are excluded automatically.
#
# Usage: deploy/ship.sh [--no-restart] [--dry-run]

set -euo pipefail

SSH_KEY="${SSH_KEY:-$HOME/.ssh/atlas_foundry_ed25519}"
SSH_PORT="${SSH_PORT:-30378}"
SSH_HOST="${SSH_HOST:-ubuntu@foundry.ai.ciscolabs.com}"
LOCAL_ROOT="${LOCAL_ROOT:-$(git rev-parse --show-toplevel)}"
REMOTE_ROOT="${REMOTE_ROOT:-/home/ubuntu/dashboard}"
SERVICE="${SERVICE:-dashboard.service}"

DO_RESTART=1
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --no-restart) DO_RESTART=0 ;;
    --dry-run) DRY_RUN=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

cd "$LOCAL_ROOT"

FILES=()
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  FILES+=("$line")
done < <(git status --porcelain | awk '
  {
    s = substr($0, 1, 2)
    p = substr($0, 4)
    if (s ~ /D/) next
    if (s ~ /R/) {
      n = split(p, a, " -> ")
      if (n == 2) p = a[2]
    }
    print p
  }
')

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "Nothing to ship — working tree is clean vs HEAD."
  exit 0
fi

echo ">> Files to ship (${#FILES[@]}):"
for f in "${FILES[@]}"; do echo "    $f"; done

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo ">> Dry run — not shipping."
  exit 0
fi

ssh_cmd() { ssh -p "$SSH_PORT" -i "$SSH_KEY" "$SSH_HOST" "$@"; }
scp_one() {
  local rel="$1"
  ssh_cmd "mkdir -p '$REMOTE_ROOT/$(dirname "$rel")'"
  scp -P "$SSH_PORT" -p -i "$SSH_KEY" "$LOCAL_ROOT/$rel" "$SSH_HOST:$REMOTE_ROOT/$rel"
}

echo ">> Shipping files..."
for f in "${FILES[@]}"; do
  echo "  -> $f"
  scp_one "$f"
done

if [[ "$DO_RESTART" -eq 1 ]]; then
  echo ">> Restarting $SERVICE..."
  ssh_cmd "sudo systemctl restart $SERVICE"
  echo ">> Service status:"
  ssh_cmd "systemctl is-active $SERVICE"
else
  echo ">> Skipping restart (--no-restart)"
fi

echo ">> Done."
