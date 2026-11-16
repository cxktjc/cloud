#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sudo install -m 0644 "$ROOT"/deploy/systemd/v9711-*.service /etc/systemd/system/
sudo systemctl daemon-reload
printf 'Units installed. Inspect with: systemctl list-unit-files | grep v9711\n'
printf 'Only v9711-ws-signal/trade units installed. Stop legacy execution and migrate state before enabling v9711-ws-trade.\n'
