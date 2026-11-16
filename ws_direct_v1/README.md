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

All runtime paths are derived from the installed code directory, never from
.env path settings or the working directory:

- Completed 3m status: ../var/klines_3m_status.json
- Account stream: ../var/binance_account_stream
- Symbols: config/symbols.txt
- New price mirror: var/binance_ticker_price/latest.json
- Precision filters: var/exchange_info_cache.json (new trade refreshes automatically)
- Live/migrated state: var/state
- Explicit --paper mode: var/paper_state (BOTH signal and trade need --paper)

Old V9711_* path settings are ignored; no private .env editing is needed to
correct those paths. Credentials and other operational settings still use .env.
The .env file itself is always loaded from this version's code directory.
Existing files/ledgers are never moved or deleted. If previously running with
V9711_LIVE_STATE_DIR pointing to var/paper_state, replace that environment
override with --paper on BOTH launch commands to continue the same ledger.

Keep the existing 3m/agg/account collectors running. Do NOT start a second
account writer or an external ticker for this version. The new trade fetches
public precision filters in a separate background thread at startup, then every
6 hours (failure retry 60s). Failed fetches retain a valid old cache; missing or
invalid filters block entry instead of guessing tick/step precision.

Diagnostics aggregate first-tick/reconnect waits and stale quotes. Detailed
plan dumps require --log-level DEBUG; real rejects and network errors remain
visible. Internal WS prices=FRESH/SUBSCRIBED distinguishes partial coverage
from an entirely disconnected feed. Group health alone does not mean every
illiquid symbol has traded since startup.

## Server preparation (does not start a live trader)

```bash
cd /opt/v9711-live/cloud
git pull --ff-only
cd ws_direct_v1
# This version can reuse the existing interpreter/dependencies.
cp -n .env.example .env
chmod 600 .env
# Edit .env: fill API keys and MySQL settings; no runtime paths needed.
nano .env
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
../.venv/bin/python -B -u -m v9711_live.run_signal --paper
```
Terminal B:
```bash
cd /opt/v9711-live/cloud/ws_direct_v1
../.venv/bin/python -B -u -m v9711_live.execution.runner --paper
```

Signal may start without a price file; trade establishes WS and publishes it.
Do not run two new trades writing the same mirror. A separate writer lock is
held for the mirror's entire lifetime. Stop the paper processes before live.

## Automatic execution-ledger takeover on live startup

Signal and execution use different books in the SAME var/state directory:
signal_portfolio.json is the model book; execution_ledger.json and
execution_intents.json record real exact-order ownership. Model positions must
never be converted into exchange fills.

Stop the OLD trade before starting the new live trade; leave account/3m/agg
running. A matching new signal already using var/state can remain running:
automatic takeover does not copy, replace, lock or delete its signal book.
A --paper signal must be stopped and launched without --paper for live plans.

The live runner now automatically:

1. Reuses a valid existing real execution pair in its own var/state, without
   requiring a manually created migration marker.
2. Otherwise detects old execution files in ../var/state and ../../var/state
   relative to ws_direct_v1 (cloud/var/state and /opt/v9711-live/var/state on
   this server). It locks legacy trade writers before reading/copying.
3. Copies ONLY the two execution files, byte-for-byte, preserving UNKNOWN
   positions/orders and realized PnL; never copies signal_portfolio.json.
4. Keeps legacy trade locks for the whole new live process lifetime and writes
   a takeover receipt automatically. A recognized interrupted automatic copy
   resumes only if source hashes and already-copied bytes still match.
5. Rejects conflicting/partial ledgers, simulated identities, dry-run history
   or missing real history with explicit paths and reasons; never guesses,
   overwrites existing execution files, imports paper or invents account fills.

The older scripts.migrate_state command remains an OPTIONAL offline tool, not
a mandatory startup step. Do not run it on top of existing state or delete files
to satisfy a marker check. Existing successful migration receipts remain valid.

Start matching live signal if not already running:

```bash
cd /opt/v9711-live/cloud/ws_direct_v1
/opt/v9711-live/cloud/.venv/bin/python -B -u -m v9711_live.run_signal
```

Then start the new trade (REAL ORDER SUBMISSION):

```bash
cd /opt/v9711-live/cloud/ws_direct_v1
/opt/v9711-live/cloud/.venv/bin/python -B -u -m v9711_live.execution.runner --enable-live --no-dry-run
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

### Switching after a previous dry-run

A dry_run feedback snapshot alone does not invalidate durable real orders.
Pure simulated execution files in var/state are backed up automatically under
var/state/execution_backups before starting a real execution book or importing
a real legacy book. This never moves/resets signal_portfolio.json. Actual
orders, owned positions, unknown client identities or mixed real/simulated
history are not erased. The backup location is printed at startup. No manual
file removal or migration marker editing is required.

### Manual external close and signal-capital synchronization

The live executor retires an exact-owned LONG position after a manual/web close
only when the account stream proves both (1) a post-entry explicit LONG=0 or a
post-entry full REST snapshot omission and (2) enough post-entry non-v9711 SELL
trade fills. It records an estimated realized PnL from the fill VWAP and configured
fees; it does not rely on a mark-price/position-absence guess. Until both proofs
arrive, the owned position is retained.

On every ready signal snapshot, execution force-syncs a separate
signal_capital_sync ledger record (signal realized balance, per-order target,
revision/session). The desired-order notional remains signal-authoritative.
Trade realized_balance remains exact execution accounting and is intentionally
NOT overwritten by model PnL. This keeps sizing synchronized without falsifying
actual fills or trade returns.

Resolved forced/manual closes stay in execution_ledger.json for audit. The panel
shows only aggregate counts, not a repeated symbol-by-symbol history. An old
exact close identity remains in cleanup until terminal exchange evidence; the
summary is not an open position.
