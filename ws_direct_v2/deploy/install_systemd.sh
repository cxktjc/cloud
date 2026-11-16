#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sudo install -m 0644 "$ROOT"/deploy/systemd/v9711-*.service /etc/systemd/system/
sudo systemctl daemon-reload
printf 'Units installed. Inspect with: systemctl list-unit-files | grep v9711\n'
printf 'Only v9711-ws-v2-signal/trade units installed. Stop legacy execution and manually cancel/close old exposure at the exchange before enabling v2; no old ledger cleanup required.\n'
