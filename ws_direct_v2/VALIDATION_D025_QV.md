# v2 d025 / dynamic QV validation — 2026-11-17

Scope: port the requested entry offset and cs_smooth QV from
`deliverables/v9891_fixed_d025_strategy_20261117/lib/strategy.py`.
Keep previous completed close as the live price reference. Exclude the
reference's 25% hard stop. This is engineering verification, not a new backtest.

## Result

- `D:\program\python.exe -B -m unittest discover -s tests -q`: **280 passed**.
- Independently imported the supplied reference and compared its numpy/pandas
  QV functions with the standard-library live implementation on synthetic
  180-bar × 647-symbol quote volumes, including 2% missing cells.
- Compared 5 fully warmed target bars: **3,235 QV20 values and 5 smoothed
  thresholds matched** (`rtol=1e-12`, `atol=1e-7`). All future candles remained
  in the input to check past-only slicing. No market-data database was queried.

## Relevant behavior covered

- Previous-close × 0.975 entry; current/future OHLC/QV cannot change its threshold.
- Positive finite cross-section q90 with linear interpolation, trailing 24 mean;
  full historical reconstruction across month boundary and restart.
- Calendar windows with per-symbol missing cells zero-filled, min_periods=10;
  a missing whole market bar blocks readiness even with >43 total timestamps.
- Bounded missing-history retry merges late rows into the 96-bar cache.
- Full configured universe remains in QV even when status initially reports only
  one completed symbol; diagnostics are published in `portfolio.qv`.
- Old `HISTORY_BARS=40` upgrades to 96; old gap overrides cannot restore the old
  2.2/2.0/1.8 tiers. New tiers: 2.0/1.8/1.6, strict >0.5pp advantage and strict
  new<1.6 / old>1.8 exception. Incumbents are not price-canceled for gap alone.
- Legacy book validation and original-byte backup; preserve session, funds,
  positions, quantities, original due times and records; cut over on the next
  full bar. Already processed legacy checkpoints do not get repriced.
- Interrupted upgrade can resume; conflicting backups/unknown parameters stop
  without discarding the original. Newly processed pre-cutover bars still
  accept late candles under legacy rules.
- Planner/model price, QV and due-time parity; bounded catch-up, restart and
  latest-bar replay; corrections inside the smoothing history affect replay.
- An entry candle crossing a hypothetical 25% stop does not trigger that stop.
- Existing 50/30/20 sizing, independent books, two extra signal-follow entries,
  six-position send gate, 1-second early exit and twelve 5-second close rounds
  remain covered by the full execution regression suite.

`BASELINE.json` is retained as the original port's historical manifest. Source
identity against that historical signal/selector is no longer asserted for
these explicitly requested changes; the behavior above is tested instead.

No production orders, server restart, production ledger edit, or strategy-return
claim was made by this validation. Runtime dependencies remain unchanged.
