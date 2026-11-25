# v27 market throughput and 3-minute diagnostics

The 435 ms ELSAUSDT queue delay and the 468 ms age of the selected 牛来USDT quote are separate measurements. The logs show those intervals, but do not identify the exact maintenance call or input burst that produced the historical ELSAUSDT queue delay.

- Ordinary entry evidence now uses `V9711_WS_ENTRY_QUOTE_MAX_AGE_SECONDS` (150 ms default, capped by the existing queue-age setting). The two sources qualify independently. This is local age at selection (and for entries already subject to a price recheck, age at that recheck); it is not a 150 ms send deadline or an exchange timestamp check. Retained near-admission and signal-follow rules, valuation, and the global queue-age policy are separate.
- Prioritize at/below-limit crossings and exact completion wakes. Keep each symbol's queued predecessors and retain crossing/rebound boundaries. An unrelated quote without an active template or quote in a redundant far run can finish without an owner decision; consecutive same-price aggTrade frames refresh ingress evidence without repeatedly waking the owner within 50 ms. Full-universe maintenance is spaced out with ranked admission in between; position/close reconciliation keeps its existing cadence. Exact order persistence still precedes send.
- `request_activity.bars_3m` in trade feedback joins market and wire activity by UTC 3-minute interval. It retains populated rows within the last 60 intervals in the current process. Entries/cancels/close are sent attempts (batch members counted separately); the one-second mutation peak counts wire requests. Queue drops, age rejection, revoked generations, local no-send, exchange rejection and rate limits are distinct. These numbers are evidence about a bar, not proof each dropped market event was an order opportunity. Queue-wait statistics use the time of processing; entry/cancel counts use send time, so an order and its subsequent cancel may belong to different bars.

## Offline pressure comparison

Two logical CPUs, 800 symbols, 130 moving near threshold, 100 ms ingress batches, 3 waves of 8 foreground opportunities, 45 seconds. Signed local socket fixture, actual local durable journal, 15-second resting minimum and 1.5-second cancel scheduler; no external exchange traffic.

| Build / offered frames per sec | Measured frames/s | Foreground sends | Trigger-to-send P95 / max | Queue peak | Overload discards / backlog expiry | Cancel sends |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| v26 / 36,000 | 36,001 | 24/24 | 48.570 / 48.763 ms | 783 | 0 / 0 | 19 |
| v27 / 36,000 | 36,001 | 24/24 | 47.251 / 47.472 ms | 781 | 0 / 0 | 19 |
| v27 / 72,000 | 67,041 | 19/24 | 45.782 / 46.006 ms (sent samples only) | 8,192 | 282,789 / 34,390 | 20 |

At 36k the queue P95 for sent orders was 28.499 ms (v26: 25.989 ms). This does not demonstrate a steady improvement in queue P95. At 72k the queue maximum was 741.502 ms and five test opportunities did not send; the good-looking latency of survivors does not indicate acceptable performance. No sent sample met the 10 ms target in these runs. The 72k result remains an overload limit, not a supported operating rate.

```powershell
& D:\program\python.exe -B -m tests.benchmark_entry_latency_v7 --samples 8 --history 1000 --signed-socket --burst --cpus 2 --near-symbols 130 --timing-history 5000 --rounds 3 --round-seconds 15 --ingress-burst-ms 100 --changing --live-cancel-pressure --pressure-rate 36000 --require-ms 10 --output var/validation_v2/v27_delivery36.json
```

Use 72,000 for overload characterization. Report files also contain per-order stages, signed cancel timing, rejected opportunity symbols, and the 3-minute diagnostics.
