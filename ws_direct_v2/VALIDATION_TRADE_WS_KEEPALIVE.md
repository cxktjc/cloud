> 历史记录：本文随 v1 工程复制，部分命令/行为属于旧版本。v2 当前路径、原生账本和验收结果以 [README.md](README.md) 与 [VALIDATION_V2.md](VALIDATION_V2.md) 为准。

# Trading WS keepalive/recovery validation ? 2026-11-16

## Change scope

Only the isolated `ws_direct_v2` package changes. The older deployment, signal
model, account collector, market subscriptions and execution ledgers are not
modified. The trade adapter no longer reconnects simply because a socket was
idle for 30 seconds. Established sockets use protocol Ping every 20s, Pong timeout
10s, background connection recovery (1..30s retry backoff), and 23h session rotation.
No per-ping INFO logging and no extra account/order API polling is introduced.

Recovery/rotation and request send/ACK share a socket lock. Background work never
replaces a socket under an in-flight application request or changes ledger state.
An ambiguous submission is never automatically replayed (including MARKET SELL
via REST). Existing exact identity reconciliation remains authoritative. Entry
validity guards run again immediately before the actual WS send, after reconnect.
Synchronous timeouts cancel their queued coroutine, preventing later hidden sends.

## Validation

Interpreter: `D:\program\python.exe`, with `-B`.

- Full suite: **172 tests passed** (`-m unittest discover -s tests -q`).
- New transport tests: **22 passed**.
- `-m compileall -q v9711_live collectors scripts tests`: passed.
- `git diff --check`: passed.
- All **72 original deployment normalized SHA256 hashes** still match BASELINE.

New coverage includes:

1. Healthy idle reuse; Ping/Pong options; no idle forced reconnect.
2. Idle disconnect recovery without new orders; reconnect-failure backoff.
3. Session rotation waits for an outstanding ACK.
4. Lost order ACK is not replayed; exact query after reconnect.
5. Post-reconnect entry-expiry guard; canceled queued/in-flight coroutine safety.
6. Supervisor/thread cleanup on shutdown and failed initial connection.
7. Real LOCAL WebSocket server tests for protocol ping/pong, deliberate missing
   pong timeout/recovery, remote disconnect, and disconnect immediately after
   order send. These tests connect only to an ephemeral 127.0.0.1 listener.
8. Adapter/executor regressions for UNKNOWN entry, cancellation and close:
   identity/holdings survive a process restart, then exact evidence settles once
   without a second submission or a blind REST fallback.

Tests use synthetic credentials, temporary state and fake/local transports.
No Binance API requests, real order submissions, server login, production ledger
writes or process restarts were performed. The fix retains order identities and
accounting; it cannot promise uninterrupted new fills during a real network outage.

## Upgrade

Pull cloud/main and restart only WS-DIRECT trade with the ordinary live command.
Do not delete/reset ledgers and do not add `--sync-signal-ledger` for this upgrade.
See the README keepalive section for expected behavior.
