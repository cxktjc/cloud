#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sudo install -m 0644 "$ROOT"/deploy/systemd/v9711-*.service /etc/systemd/system/
sudo systemctl daemon-reload
printf 'Units installed. Inspect with: systemctl list-unit-files | grep v9711\n'
printf 'Enable collectors first; start v9711-trade.service only after verification.\n'
