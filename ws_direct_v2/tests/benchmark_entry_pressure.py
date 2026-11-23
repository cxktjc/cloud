"""Repeat open-loop entry pressure cases. Offline only; no exchange access.

python -B -m tests.benchmark_entry_pressure --output var/validation_v2/entry_pressure.json
"""
import argparse
import json
from pathlib import Path

from tests.benchmark_entry_latency_v7 import restrict_cpus, run, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rates', default='4800,9600,14400,24000')
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--cpus', type=int, default=2)
    parser.add_argument('--burst', action='store_true')
    parser.add_argument('--changing', action='store_true')
    parser.add_argument('--disk-delay-ms', type=float, default=0.)
    parser.add_argument('--ingress-burst-ms', type=float, default=0.)
    parser.add_argument('--near-symbols', type=int, default=0)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--require-ms', type=float)
    args = parser.parse_args()
    rates = [int(value) for value in args.rates.split(',')]
    if not rates or min(rates) < 1 or not 1 <= args.trials <= 20 or not 1 <= args.cpus <= 19:
        parser.error('positive rates, cpus in 1..19 and trials in 1..20 required')
    if not 0 <= args.disk_delay_ms <= 1000:
        parser.error('disk-delay-ms must be in 0..1000')
    if not 0 <= args.ingress_burst_ms <= 1000:
        parser.error('ingress-burst-ms must be in 0..1000')
    if not 0 <= args.near_symbols <= 792:
        parser.error('near-symbols must be in 0..792')
    affinity = restrict_cpus(args.cpus)
    result = dict(scope='OFFLINE: real durable files, owner maintenance, 19 workers, broker signing/JSON; '
        'fake socket, no TLS/network RTT; mocked account health/order evidence and terminal; '
        '~54KB padding plus active state. Process affinity is not equivalent to a cloud CPU quota.',
        cpu_affinity=affinity, burst=args.burst, changing=args.changing, disk_delay_ms=args.disk_delay_ms,
        ingress_burst_ms=args.ingress_burst_ms, near_symbols=args.near_symbols, cases=[])
    failed = False
    latency_failed = False
    for rate in rates:
        trials = []
        for index in range(args.trials):
            value = run(8, 0, signed_socket=True, burst=args.burst, pressure_rate=rate, changing=args.changing,
                        disk_delay_ms=args.disk_delay_ms, ingress_burst_ms=args.ingress_burst_ms,
                        near_symbols=args.near_symbols)
            trials.append(value)
            evidence = value['pressure']
            failed |= bool(evidence['errors'] or evidence['unsent_symbols'] or evidence['duplicate_symbols']
                or evidence.get('duplicate_client_ids')
                or evidence['counters']['overflow'] or evidence['counters']['overload_discarded']
                or evidence['counters']['rejected_backlog'] or evidence['achieved_raw_rate'] < .9*rate)
            print(json.dumps(dict(rate=rate, trial=index+1, latency=value['latency'],
                                  actual_ingress=evidence['achieved_raw_rate'],
                                  actual_enqueued=evidence['achieved_effective_rate'])), flush=True)
        combined = summary([send['ms'] for trial in trials for send in trial['sends']])
        all_orders = summary([send['ms'] for trial in trials
            for send in trial['sends']+trial.get('background_sends', [])])
        latency_failed |= args.require_ms is not None and (combined['samples'] != 8*args.trials
            or all_orders['max_ms'] > args.require_ms)
        result['cases'].append(dict(rate=rate, latency=combined, all_orders_latency=all_orders,
            all_sends_within_10ms=(combined['samples'] == 8*args.trials
                and all_orders['samples'] == all_orders['within_10ms']), trials=trials))
        result['integrity_or_load_failure'] = failed
        result['latency_target_ms'] = args.require_ms
        result['latency_target_passed'] = not latency_failed
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(dict(summary=[dict(rate=c['rate'], **c['latency']) for c in result['cases']],
                          integrity_or_load_failure=failed)), flush=True)
    return 1 if failed else 2 if latency_failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
