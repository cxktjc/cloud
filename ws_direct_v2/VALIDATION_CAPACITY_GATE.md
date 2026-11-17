# Full-three-position admission audit — 2026-11-17 UTC

Report: several entry orders remained visible after trade reached three
positions. The original screen/time/due times were not retained, and production
state was not available locally. This audit checks code paths and reproduces
the reported duration; it cannot classify that particular live observation.

Result: **289 tests passed**, including 8 new offline integration checks in
`tests/test_capacity_gate_v2.py`. No runtime, display, strategy or pacing changes.

- Three owned positions outside the final-pre-exit window admit no new ordinary
  entries. An explicit owner submission attempt is also rejected.
- Recording the third exact fill immediately updates the worker gate. Both
  a queued single send and a queued batch are rejected before broker submission.
- Existing ordinary orders receive cancellation requests. A missing cancel ACK
  keeps exact identities and occupied slots; it does not permit replacements.
- With three positions, the final-pre-exit exception starts at 180 seconds
  remaining and ends at the original due instant. A worker checks the original
  timestamps at send time; it cannot extend a previously granted exception.
- Full ordinary capacity still permits the two independent signal-follow orders
  requested by the user; only ordinary orders are canceled in that case.
- The real background cancellation scheduler was exercised with a virtual
  monotonic clock: after seven confirmed cancels at 9.0 seconds, one of eight
  ordinary orders remained tracked; the final cancel could start at 10.5 seconds.
  This 1.5-second interval after each response is intentional and unchanged.

The terminal's pending-order counts include retained tracked identities until
confirmed terminal evidence removes them. The existing ordinary order summary
does not show `cancel_requested` per row; the cancellation queue is displayed
separately. Thus several visible orders alone do not prove that full capacity
admitted new orders. Signal-follow rows carry a separate label/count.

The live incident could have been pending cancellation, the final-pre-exit
exception, or signal-follow orders. No definite incident cause is claimed
without the contemporaneous orders, due times and UTC timestamp.
