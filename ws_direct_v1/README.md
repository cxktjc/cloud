# v9711 WS-direct v1 (isolated new version)

This directory is a complete copy of the audited cloud execution stack at the
commit in BASELINE.json, plus a new WS-driven entry path and matching signal
contract. No imports of the parent strategy packages are required. Legacy
signal/trade source files are not modified by this version.

## Critical behavior

```
completed 3m -> signal full-bar candidate templates -> desired_orders_ws.json
exchange aggTrade -> bounded FIFO -> trade owner incremental admission/dispatch
                                 -> committed price cache -> async latest.json
                                                          -> signal model/UI
```

Signal keeps the original model ledger, completed-bar entry/exit semantics,
compounding and near-candidate display. It additionally publishes ALL valid
current-bar candidate templates, excluding model-held symbols, even when there
is no price snapshot or no candidate within the discovery threshold. A template
is NOT an order. Trade's internal WS prices drive the original tiered eight-order
selector. The legacy desired[] list is not a prerequisite for admission.

Every accepted tick gets its own decision event (no pre-decision last-price
coalescing that would erase a brief threshold crossing). A price enters the file
mirror only AFTER its owner callback has evaluated and dispatched any admission.
This does not wait for exchange ACK. The writer can coalesce processed prices
at 100ms; slow or broken disk output cannot make trade wait for its own mirror.
Trade never reads latest.json to decide an entry.

Signal plans still use a file: this happens ahead of entry, not a per-tick
signal round trip. The executor validates mode/version, lease, active bar and
readiness every decision. It rejects legacy contracts. New 3m preparation/data
availability latency is not eliminated by the WS path.

### Preserved safety

- Original 5/8 tiered admission, cancel-first rotation, position capacity and
  final-pre-exit-bar exception; single owner for all ledger/capacity mutations.
- Existing independent singleton/batch submission workers, no wait to collect a
  batch, no global ACK barrier. Singletons use the original WS adapter.
- Precision filters, minimum sizing, exact identity, QUEUE close fallback,
  historical liquidation/ADL recovery and no forced-exit replenishment.
- Durable intent registration remains BEFORE sending. This unavoidable disk
  work and OS scheduling still cost time: this is NOT a zero-latency claim.
- All WS groups require actual data, reconnect generation invalidates old
  queued quotes, worker-side guards re-check market/account readiness.
- Bounded FIFO overflow disconnects/fails closed. Old queued events are not
  replayed as current trading opportunities. Counters expose rejected backlog.
- Account/close safety maintenance runs at most 250ms apart between decisions
  (subject to the duration of its work); it is not an admission polling timer.
- UI remains changed-only, max once per 3 seconds. Feedback entry_diagnostics
  includes ws_direct receive-to-decision percentiles, actual admission-dispatch
  samples, queue depth and overflow/stale counters. These exclude exchange ACK.
- WS data is an overwritten snapshot, not an unbounded tick-history database.

## Files / shared inputs

New writer-owned files default to this directory's var/:

- var/state/desired_orders_ws.json (matching new signal only)
- var/state/signal_portfolio.json (new/migrated model ledger)
- var/state/execution_ledger.json and execution_intents.json
- var/state/execution_feedback.json, trade.log, signal.log
- var/binance_ticker_price/latest.json (ONLY new trade writes it)

.env.example shares the existing completed-3m status/MySQL and live account
stream in /opt/v9711-live/cloud/var. Adjust the READ paths to the actual running
collectors. Keep the existing 3m/agg/account processes running. Do NOT start a
second account writer or the copied ticker collector. The ticker module remains
as the internal WS transport implementation, not an external price publisher.

The new process rejects a legacy V9711_DEPLOY_ROOT or state/price writer override
pointing outside this directory. Never copy an old .env unchanged. The process
loads its own .env; shell variables override it and may need to be unset.

## Server preparation (does not start a live trader)

```bash
cd /opt/v9711-live/cloud
git pull --ff-only
cd ws_direct_v1
# This version can reuse the existing interpreter/dependencies.
cp -n .env.example .env
chmod 600 .env
# Edit .env: fill API keys, MySQL settings and real shared input paths.
# Do NOT change this version's writer paths back to the legacy directory.
nano .env
unset V9711_DEPLOY_ROOT V9711_LIVE_STATE_DIR V9711_LIVE_PRICE_FILE
../.venv/bin/python -B -m v9711_live.execution.runner --check-inputs
../.venv/bin/python -B -m v9711_live.run_signal --check-inputs
../.venv/bin/python -B -m scripts.refresh_exchange_info
```

No new Python dependency is added beyond requirements.txt. An independently
installed interpreter must install that file. Avoid pip-installing both same-name
v9711_live packages globally: always launch from the chosen version directory.

## Optional paper trial while legacy live remains running

Use a SEPARATE paper state directory in BOTH terminals. No --enable-live flag.

Terminal A:
```bash
cd /opt/v9711-live/cloud/ws_direct_v1
V9711_LIVE_STATE_DIR="$PWD/var/paper_state" ../.venv/bin/python -B -u -m v9711_live.run_signal
```
Terminal B:
```bash
cd /opt/v9711-live/cloud/ws_direct_v1
V9711_LIVE_STATE_DIR="$PWD/var/paper_state" ../.venv/bin/python -B -u -m v9711_live.execution.runner
```

Signal may start without a price file; trade establishes WS and publishes it.
Do not run two new trades writing the same mirror. A separate writer lock is
held for the mirror's entire lifetime. Stop the paper processes before live.

## Preserve live ownership on cutover (mandatory)

Stop the OLD trade AND old signal; leave account/3m/agg running. Confirm there
is no remaining old executor, including systemd auto-restarts. Do not cancel or
forget exchange orders manually just to switch code versions.

Earlier server logs used /opt/v9711-live/var/state as the legacy state directory,
NOT /opt/v9711-live/cloud/var/state. Confirm your actual current path before using
--source. Default below is only the path seen in those logs.

```bash
cd /opt/v9711-live/cloud/ws_direct_v1
../.venv/bin/python -B -m scripts.migrate_state --source /opt/v9711-live/var/state
# Inspect preview, then copy with both old processes stopped:
../.venv/bin/python -B -m scripts.migrate_state --source /opt/v9711-live/var/state --apply
```

The script locks both old/new signal/trade state owners, copies execution
intents/ledger and signal portfolio byte-for-byte, never deletes/moves source,
and refuses existing destination state. It does NOT copy old desired[] plans.
UNKNOWN positions/close identities/forced-close audit are preserved. The new
live process also holds the original source trade.lock for its entire lifetime,
blocking an old executor auto-restart while the new trader is running. A partial
migration leaves a marker that blocks live startup. Live mode requires a
successful WS_MIGRATION.json marker; it will not silently start an empty ledger.
If a paper trial already used the LIVE var/state by mistake, do not delete it
blindly: inspect it instead of forcing an overwrite.

Then in separate terminals, without the paper STATE_DIR override:

```bash
# New matching signal:
cd /opt/v9711-live/cloud/ws_direct_v1
../.venv/bin/python -B -u -m v9711_live.run_signal
```

```bash
# REAL ORDER SUBMISSION: only after migration and input checks.
cd /opt/v9711-live/cloud/ws_direct_v1
../.venv/bin/python -B -u -m v9711_live.execution.runner --enable-live --no-dry-run
```

No old trade should be running beside it. If reverting after new fills, stop
both new processes and reconcile/migrate the NEW current ledger back deliberately;
never simply start the old stale ledger, which no longer knows the new orders.

## Optional systemd

Only v9711-ws-signal.service and v9711-ws-trade.service are included. They do not
replace old unit names. Inspect User= and file permissions for shared SQLite and
new writable var/. Install via deploy/install_systemd.sh, then start only after
cutover. Do not enable two traders. The templates reuse cloud/.venv/bin/python.

## Offline verification

```bash
../.venv/bin/python -B -m unittest discover -s tests -q
../.venv/bin/python -B -m compileall -q v9711_live collectors scripts
```

Tests mock order transports. They cover priority over the mirror, blocked mirror
I/O, transient crossings, reconnect/stale/overflow guards, direct entry with no
price file and an empty legacy desired[], no duplicate entries, full-plan mode/
lease checks, incremental/full selector parity, precision, liquidation/UNKNOWN
recovery, safe migration and original model/policy source hashes. No live orders
or production state are touched by these tests. Server/network latency and live
exchange acceptance still require separate supervised validation.
