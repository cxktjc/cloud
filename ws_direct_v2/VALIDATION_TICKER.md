> 历史记录：本文随 v1 工程复制，部分命令/行为属于旧版本。v2 当前路径、原生账本和验收结果以 [README.md](README.md) 与 [VALIDATION_V2.md](VALIDATION_V2.md) 为准。

# Ticker silent-connection fix

Scope: standalone deployment only; no live orders or production state changed.

Findings:
- Legacy combined-stream base differed from the existing 3m collector route.
- Transport handshake previously marked a group ready without market data.
- JSON heartbeat previously refreshed last_success_epoch without a market receipt.

Changes: explicit endpoint logging, compatibility endpoint rotation after first-data timeout;
readiness only after a valid recent trade for a subscribed group symbol; 15s first-data
and at most 30s valid-data silence watchdog; heartbeats preserve receipt timestamps.

Verification: 9 offline unittest cases passed, including a silent mock socket followed
by recovery on another endpoint, no readiness on handshake alone, heartbeat TTL,
Unicode combined-stream names, and direct networking with hostile proxy env variables.

Live read-only probe: local direct connections to all three production paths timed out
in opening handshake. This does NOT establish which endpoint works on the user's Ubuntu
server. Official docs fetch returned HTTP 202 with empty content. No claim of verified
live endpoint availability is made. Confirm DATA READY on the server after restart.
