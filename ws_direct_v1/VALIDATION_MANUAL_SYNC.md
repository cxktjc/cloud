# Manual signal-to-trade ledger sync validation (2026-11-16)

## Scope

Only `ws_direct_v1` changes. Removes the per-ready-snapshot signal capital-sync
record. Adds the explicit one-shot `--sync-signal-ledger` runner flag (apply and
exit, no exchange client) and preview/apply `scripts.sync_signal_to_trade` CLI.
Normal trade startup/resume does not import signal capital or holdings.

The import copies the signal initial equity, realized balance and selected LONG
positions, including entry basis, quantity and original exit due time. Imported
positions have a separate operator-seed identity, never a fabricated exchange
entry client ID. No exchange orders are submitted by the importer. Operators
must verify actual exchange holdings/quantities before adopting model positions.

After import, new-entry notional uses trade's own realized balance divided by
three. Unchanged in-flight order notionals stay fixed. Later signal balance or
target updates do not replace the owned trade positions/balance. Trade fills and
closes, including post-import manual/forced closes, update its independent ledger.

## Offline checks

Windows interpreter: `D:\program\python.exe`, with `-B`.

- `-m unittest discover -s tests -q`: **150 tests passed**.
- `-m compileall -q v9711_live collectors scripts tests`: passed.
- `git diff --check`: passed.
- Both manual-sync CLI `--help` commands: passed.
- Original parent deployment: **all 72 normalized SHA256 hashes match** the
  frozen `BASELINE.json`; no parent collector/signal/trade file changed.

New checks cover preview preserving accounting bytes; full original-file
backups; importing empty/nonempty holdings; preserving exit time; optional entry
price override with constant notional; explicit selection above the trade cap;
rejecting active orders, UNKNOWN closes, orphan intents, incompatible live/paper
history and writer-lock conflicts; legacy writer protection; interrupted apply
blocking startup and safe frozen-target recovery; no fake fills in session stats;
independent own-balance sizing in both direct ticks and maintenance; own close
PnL and restart persistence; operator-seed manual and forced exits; normal live
restart not resyncing; and explicit runner import exiting before client creation.

## Production boundaries

No server login, production ledger import, live BUY/SELL, or production process
restart was performed during validation. Tests use temporary directories and
mocked/simulated transports. Passing tests do not validate the user's actual
exchange holdings; accounting import alone does not open/close exchange positions.

See README.md for stop, preview, one-shot apply, backup and ordinary restart
commands. Never leave the one-shot sync flag in systemd/regular startup commands.
