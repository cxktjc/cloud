# Entry latency: two logical CPUs, changing quotes, 2026-11-26

Offline Windows run with real temporary journal syncs, signed JSON and a fake socket. No exchange connection or live deployment. The latency budget is 20 ms to local send, not ACK. Per physical order take the largest of scheduled input-to-send, recorded trigger-to-send, and basis quote-to-send when available. Count ordinary background sends too. Missed target opportunities, duplicated IDs, overflows, stale backlog and inadequate raw rate fail integrity independently.

|Scenario|Targets sent/scheduled|All sends|P50 ms|P95 ms|Maximum ms|Over 20 ms|Expired backlog|
|---|---:|---:|---:|---:|---:|---:|---:|
|8644330 baseline, 6 rounds|48/48|83|27.507|85.147|86.136|57|0|
|Final, 6 rounds|48/48|83|21.686|46.984|98.184|44|0|
|Final, 20 rounds|159/160|264|70.364|398.601|509.133|248|41910|

The six-round figures are recomputed from the raw per-order stages. The longer run changes the background universe (800 minus scheduled target symbols) and should not be treated as a before/after comparison. `--changing` alternates each background symbol's price; effective queue load can differ even at the same 36k raw frames/s. No duplicated targets or CIDs in the longer run; one target N151USDT never sent (exact reason not established). The worst background order N219USDT spent 490.204 ms from wake to dequeue and 17.016 ms from decision to send. Peak queue 5942, zero overflow. The six-round worst order had one journal sync of 25.349 ms. Do not claim a 20 ms max. No delay injection; there is no upper bound for an OS disk sync. Metadata was one slow maintenance stage (112.226 ms including scheduling); the cached metadata reader still stats the file on the owner during maintenance, and only rereads JSON on a changed stamp. These timings do not isolate filesystem time from scheduling.

Diagnostics now evict the actual oldest of 2048 rows in constant time (separate 512-row summary). On 6000 unique plans, P95 registration first/last 1000 changed from 3.1/91.3 us to 2.3/2.6 us, same host. Removed feed-lock / queue-mutex inversion on overflow, repeated ranking of identical prices and unnecessary cancel-queue scans before the next journal write. Forced/plan restrictions are still checked. No change to quote age, durability, order identity, cancel age, or WS reconnect policy.

Reproduce from ws_direct_v2 (replace 20 rounds with 6 for the paired run):

```
python -B -m tests.benchmark_entry_latency_v7 --samples 8 --rounds 20 --round-seconds 15 --history 1000 --timing-history 5000 --pressure-rate 36000 --cpus 2 --near-symbols 130 --burst --signed-socket --changing --require-ms 20 --output var/validation_v2/20261126_tail_soak.json
```

The pressure harness freezes wall time unless using the separate `--live-cancel-pressure` scenario. Do not use it to infer live wall-clock cancel timing. In the local validation directory, `20261126_tail_before.json`, `20261126_tail_final.json` and `20261126_tail_soak.json` contain full per-order timing evidence.
