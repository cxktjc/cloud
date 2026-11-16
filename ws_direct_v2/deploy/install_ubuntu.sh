#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

sudo apt-get update
sudo apt-get install -y "$PYTHON_BIN" "${PYTHON_BIN}-venv" "${PYTHON_BIN}-dev" python3-pip mysql-client ca-certificates
"$PYTHON_BIN" -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/python" -m pip install --upgrade pip wheel
"$ROOT/.venv/bin/pip" install -r "$ROOT/requirements.txt"
mkdir -p "$ROOT/var/state" "$ROOT/var/binance_ticker_price" "$ROOT/var/binance_account_stream" "$ROOT/var/log"
if [[ ! -f "$ROOT/.env" ]]; then
  cp "$ROOT/.env.example" "$ROOT/.env"
  chmod 600 "$ROOT/.env"
  echo "Created $ROOT/.env; fill in MySQL and Binance credentials before starting services."
fi
chmod +x "$ROOT/deploy"/*.sh
printf 'Python environment ready: %s\n' "$ROOT/.venv/bin/python"
printf 'Next: edit .env, run deploy/init_mysql.sh, then install deploy/systemd/*.service.\n'
