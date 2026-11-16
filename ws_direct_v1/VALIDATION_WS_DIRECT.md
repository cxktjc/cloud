# Offline validation - 2026-11-16 UTC

- New version: 81 unittest tests passed, including 28 new WS/template/migration
  cases and the copied precision/forced-close/order-identity regression suite.
- Original deployment: 53 tests passed in a separate interpreter invocation.
- Python compileall passed for the complete new bundle.
- Both signal and trade --check-inputs entrypoints executed read-only. New local
  market/account files were absent as expected: no real collectors were started.
- SHA-256 comparison of all 72 original tracked files against BASELINE.json
  found no changes. Original model, gap selection, close and forced-exit core
  files in the new copy also match normalized source hashes (Git line endings
  may differ between Windows and Ubuntu).
- Order transport tests use simulation/mocks and temporary directories. No live
  orders, production ledgers, MySQL writes or account changes were performed.

## New behavior covered

- Quote decision/dispatch precedes mirror commit, including during a blocked
  file write. Failed callbacks do not publish an unprocessed quote.
- A transient threshold crossing is delivered, not last-price-coalesced away.
- Direct entry works without any local price file and with empty legacy desired[].
- All candidate templates are published before quotes arrive; held model symbols
  are excluded and readiness still gates authorization.
- Incremental shortlist and full selector agree on a targeted rotation case and
  fifty seeded market/capacity/cancellation states.
- Repeated ticks preserve exact identity; below-limit prices are not model fills.
- Expired/wrong-contract/future-bar/not-ready plans are blocked.
- Reconnect generation, timestamp regression, overflow and stale queued messages
  fail closed. Worker market/account guards run again before submission.
- Migration copies existing UNKNOWN/ownership ledgers byte-for-byte, refuses an
  existing destination and fails when the legacy writer holds its lock.
- Live startup without a completed migration fails before opening the market WS.
- The migrated original executor lock is acquired for the entire new live run;
  an already-running legacy writer prevents any new live constructor.

## Not claimed

No real VPS latency, throughput, Binance connection availability, live fill,
slippage or financial return has been validated. Local fsync, maintenance, JSON
parsing and thread scheduling still take time. Exchange ACK latency is not the
receive-to-decision/dispatch metric. The implementation removes per-tick price
file publication and signal round trips, not every possible source of latency.

## 2026-11-16 fixed-path / startup diagnostic correction

- All runtime paths derive from code location. Stale .env/shell path variables
  cannot redirect the signal, price mirror, account input or precision cache.
- Explicit --paper on both processes retains var/paper_state. No state files
  are moved, deleted or migrated by this correction.
- Public precision filters refresh on a separate worker; the tick callback does
  not wait for HTTP. Invalid/missing precision remains fail-closed. Failed
  refresh retains the previous cache. Readiness checks run at startup/maintenance,
  not as an extra file read per tick.
- First/reconnect-tick waits and stale quotes are separate diagnostic reasons;
  the panel summarizes them and preserves actionable preflight errors.
- Full plan dumps require DEBUG (and are not serialized at normal INFO level).
- WS metrics report fresh/subscribed symbol coverage. Group health is not a
  guarantee that every subscribed symbol already delivered its first trade.
- 90 local unit tests passed, including background-refresh failure/nonblocking,
  stale environment path isolation, paper continuity and diagnostic tests.
- No live order submission or real server/network verification was performed.

## 2026-11-16 automatic execution takeover

The CLI now prepares live ownership automatically under the destination trade
lock, before constructing the executor. It discovers the fixed legacy paths,
locks old executors for the entire live lifetime, and copies only real execution
intents/ledger. The matching running signal book is never replaced. Existing
valid native execution state no longer needs a manually produced receipt.
Conflicting/partial/simulated state remains fail-closed. New automatic partial
copies resume only with matching source hashes and matching destination bytes.
The low-level DirectTrade loop still rejects bypassing CLI startup preparation.

104 local tests passed, including automatic takeover with UNKNOWN state, native
reuse, source/destination conflict, active legacy lock, incomplete ledger, paper
exclusion, simulated identity and dry-run rejection, recognized interrupted copy
recovery, foreign journal preservation and refusal to treat a signal book as
proof of exchange ownership. No server login, account query or real trade ran.

## 2026-11-16 dry-run display snapshot correction

Feedback mode is not durable order evidence. A stale dry_run feedback no longer
blocks a real/native execution ledger. Pure simulated native execution history
is fully backed up under var/state/execution_backups before switching to real
legacy execution state, or an empty live execution book when only confirmed
simulated identities exist. Signal state is untouched. Mixed real/unknown and
simulated histories are never reset. Interrupted switches resume only after
backup/manifest, legacy hashes and current file bytes validate. Later real
changes are never overwritten. 114 local tests passed; no live orders ran.
