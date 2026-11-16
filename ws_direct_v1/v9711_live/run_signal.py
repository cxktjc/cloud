r"""Persistent, signal-authoritative v9711 strategy process.

Run from the deployment root: python -B -u -m v9711_live.run_signal
Reads only local completed 3m data and ticker JSON; no exchange API or WS.
Signal positions, model returns, compounding and desired orders do not depend
on execution_feedback.json. Publishes additive target state (schema 1 / contract 2).
"""
from __future__ import annotations

from dataclasses import asdict
import argparse
import logging
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PACKAGE_DIR = Path(__file__).resolve().parent

from . import config as cfg
from .contracts import BarStats, Decision, DesiredOrder
from .market_data import BarStore, PriceReader, PriceSnapshot, StatusReader
from .local_poll import LocalChangePoller, probe_sleep
from .publisher import DesiredPublisher
from .signal_logic import SignalInput, build_candidates, long_gap, select_orders
from .signal_portfolio import SignalPortfolio
from .signal_diagnostics import CandidateDistance, nearest_candidates, nearest_candidates_line
from .terminal_view import TerminalView, desired_map, latest_completed_positions

LOG = logging.getLogger('v9711_live_signal')
PROXIMITY_LOG = logging.getLogger('v9711_live_signal.proximity')
PLAN_LOG = logging.getLogger('v9711_live_signal.plan')


class LiveSignal:
    def __init__(self, *, once: bool = False):
        cfg.STATE_DIR.mkdir(parents=True, exist_ok=True)
        self.status_reader = StatusReader(cfg.STATUS_FILE)
        self.price_reader = PriceReader(cfg.PRICE_FILE)
        self.bar_store = BarStore()
        self.portfolio = SignalPortfolio(cfg.STATE_DIR / cfg.SIGNAL_BOOK_FILENAME)
        self.publisher = DesiredPublisher(cfg.DESIRED_FILE)
        self.view = TerminalView()
        self.stats = BarStats()
        self.current_status = None
        self.current_price: PriceSnapshot | None = None
        self.candidates: tuple[Any, ...] = ()
        self.desired: dict[str, DesiredOrder] = {}
        self.nearest_candidates: tuple[CandidateDistance, ...] = ()
        self._last_logged_proximity: tuple[int, str] | None = None
        self.total_place = self.total_cancel = self.total_bars = 0
        self.total_expire = self.total_withdraw = self.total_entry_confirmed = 0
        # Ended intents are NOT cancels. Wait for their completed candle/model.
        self._ended_desired: dict[tuple[str, int], dict[str, Any]] = {}
        self.total_price_updates = self.total_evaluations = 0
        self.warmup_bars = 0
        self.warmup_ready = self.price_healthy = self.model_ready = False
        self.once = bool(once)
        self._candidate_key: tuple | None = None
        self._barstore_status_signature: tuple | None = None
        self._barstore_updated_epoch = float('nan')
        self._barstore_retry_epoch = self._model_retry_epoch = 0.0
        self._last_model_input_key: tuple | None = None
        self._last_price_freshness_key: tuple | None = None

    def _add_plan_decision(self, decision: Decision, *, order_bar_open_ms: int | None = None) -> None:
        self.stats.add(decision)
        if decision.action in {'place', 'cancel', 'expire', 'withdraw', 'entry_confirmed'}:
            name = 'total_' + decision.action
            setattr(self, name, getattr(self, name) + 1)
        if decision.action != 'keep':
            PLAN_LOG.info('signal_plan action=%s symbol=%s reason=%s order_bar=%s reporting_bar=%s limit=%s observed=%s gap=%s',
                          decision.action, decision.symbol, decision.reason,
                          self.stats.bar_open_ms if order_bar_open_ms is None else order_bar_open_ms,
                          self.stats.bar_open_ms, decision.limit_price, decision.observed_price, decision.gap)

    def _retire_desired(self, *, now: float, before_bar_ms: int | None = None) -> bool:
        """Remove old-bar intents from executable output, without calling them cancels."""
        retired = False
        for symbol, order in list(self.desired.items()):
            if (now * 1000 >= order.order_bar_available_ms
                    or (before_bar_ms is not None and order.order_bar_open_ms < before_bar_ms)):
                key = (symbol, order.order_bar_open_ms)
                self._ended_desired.setdefault(key, {'order': order, 'outcome': None, 'stats_bar': None})
                self.desired.pop(symbol)
                retired = True
        self.stats.desired_orders = len(self.desired)
        self.stats.pending_confirmation = sum(row['outcome'] is None for row in self._ended_desired.values())
        return retired

    def _resolve_ended_desired(self, *, processed_data=None, processed_from_ms: int | None = None,
                               model_events=()) -> bool:
        """Classify expiry only after the model processes that completed entry bar.

        Late/corrected latest-bar data can move expire -> entry_confirmed (or
        back) without double counting. Missing per-symbol candles stay pending.
        No-entry expiry requires a candle actually supplied to the model in its
        processed/replayable range, including temporary catch-up windows.
        """
        if not self._ended_desired:
            self.stats.pending_confirmation = 0
            return False
        through = self.portfolio.last_bar
        if through is None:
            return False
        model_entries = {(p['symbol'], p['entry_bar_open_ms']) for p in (
            list(self.portfolio.positions.values()) + self.portfolio.state['recent_trades'])}
        model_entries.update((e['symbol'], e['entry_bar_open_ms'])
                             for e in model_events if e['action'] == 'entry')
        changed = False
        for key, record in list(self._ended_desired.items()):
            symbol, bar_ms = key
            if through < bar_ms:
                continue
            order = record['order']
            if key in model_entries:
                outcome = 'entry_confirmed'
            elif (processed_data is not None and processed_from_ms is not None
                  and processed_from_ms <= bar_ms <= through):
                candle = next((b for b in processed_data.get(symbol, ()) if b.open_time_ms == bar_ms), None)
                valid_candle = candle is not None and all(math.isfinite(v) and v > 0 for v in
                                                          (candle.high, candle.low, candle.close))
                outcome = 'expire' if valid_candle else None
            elif record['outcome'] is not None:
                outcome = record['outcome']
            else:
                # A cached candle arriving AFTER the model moved past this bar
                # is not proof that the model evaluated it. Keep this unknown.
                continue
            previous = record['outcome']
            if previous != outcome:
                if previous is not None:
                    name = 'total_' + previous
                    setattr(self, name, getattr(self, name) - 1)
                    if record['stats_bar'] == self.stats.bar_open_ms:
                        setattr(self.stats, previous, getattr(self.stats, previous) - 1)
                if outcome is not None:
                    reason = ('completed_bar_entry_confirmed' if outcome == 'entry_confirmed'
                              else 'completed_bar_no_model_entry')
                    self._add_plan_decision(Decision(symbol, outcome, reason, order.limit_price),
                                            order_bar_open_ms=bar_ms)
                else:
                    PLAN_LOG.info('signal_plan action=pending_confirmation symbol=%s order_bar=%s reason=completed_candle_unavailable',
                                  symbol, bar_ms)
                record.update(outcome=outcome, stats_bar=self.stats.bar_open_ms)
                changed = True
            # Only the latest committed candle is replayable by the model.
            if outcome is not None and bar_ms < through:
                self._ended_desired.pop(key)
        self.stats.pending_confirmation = sum(row['outcome'] is None for row in self._ended_desired.values())
        return changed

    def _order_equity(self) -> float:
        return self.portfolio.balance

    def _status_is_usable(self, status, *, now: float | None = None) -> bool:
        if status is None or status.open_time_ms % cfg.BAR_MS:
            return False
        now = time.time() if now is None else float(now)
        available = (status.open_time_ms + cfg.BAR_MS) / 1000
        return available - 5 <= now < (status.open_time_ms + cfg.ORDER_BAR_LIFETIME_MS) / 1000

    def _advance_book(self, status, *, now: float) -> bool:
        before = self.portfolio.revision
        pending = [p for p in self.portfolio.positions.values()
                   if self.portfolio.last_bar is not None
                   and p['exit_bar_open_ms'] <= self.portfolio.last_bar]
        key = (status.open_time_ms, status.symbols, self.bar_store.revision)
        behind = self.portfolio.last_bar != status.open_time_ms
        if key == self._last_model_input_key and not behind and not pending:
            return False
        if key == self._last_model_input_key and now < self._model_retry_epoch:
            return False
        first = (status.open_time_ms if self.portfolio.last_bar is None
                 else self.portfolio.last_bar + cfg.BAR_MS)
        through = min(status.open_time_ms, first + (cfg.SIGNAL_CATCHUP_BARS - 1) * cfg.BAR_MS)
        required = min(first, through, self.portfolio.last_bar if self.portfolio.last_bar is not None
                       else through) - cfg.QV_WINDOW * cfg.BAR_MS
        cached_start = min((b.open_time_ms for rows in self.bar_store.bars.values() for b in rows),
                           default=status.open_time_ms)
        data = self.bar_store.bars
        if required < cached_start:
            data = self.bar_store.read_window(
                set(status.symbols) | set(self.portfolio.positions), required, through)
        if pending:
            # An old/delisted symbol's missing due close must not force a
            # repeated all-universe query spanning the entire stalled interval.
            data = {symbol: list(series) for symbol, series in data.items()}
            for pos in pending:
                symbol, due = pos['symbol'], pos['exit_bar_open_ms']
                if not any(b.open_time_ms == due for b in data.get(symbol, ())):
                    recovered = self.bar_store.read_window([symbol], due, due)
                    data.setdefault(symbol, []).extend(recovered.get(symbol, ()))
        checked_from = through if self.portfolio.last_bar is None else self.portfolio.last_bar
        events = self.portfolio.advance(data, through_bar_open_ms=through, symbols=status.symbols)
        outcomes_changed = self._resolve_ended_desired(
            processed_data=data, processed_from_ms=checked_from, model_events=events)
        for event in events:
            action = event['action']
            self.stats.add(Decision(event['symbol'], action, 'signal_completed_3m_model',
                                    event.get('entry_price')))
        self.stats.signal_entries = sum(
            p['entry_bar_open_ms'] == status.open_time_ms for p in self.portfolio.positions.values())
        self.stats.signal_exits = sum(
            p['exit_bar_open_ms'] == status.open_time_ms for p in self.portfolio.state['recent_trades'])
        self._last_model_input_key = key
        still_behind = self.portfolio.last_bar != status.open_time_ms
        progressed = self.portfolio.revision != before
        self._model_retry_epoch = now + (0.0 if still_behind and progressed else max(0.25, cfg.DB_SETTLE_SECONDS))
        return progressed or outcomes_changed

    def _refresh_bar(self, status, *, now: float | None = None) -> bool:
        if status is None:
            return False
        now = time.time() if now is None else float(now)
        if self.stats.bar_open_ms != status.open_time_ms + cfg.BAR_MS:
            self._retire_desired(now=now, before_bar_ms=status.open_time_ms + cfg.BAR_MS)
            self.stats.reset(status.open_time_ms + cfg.BAR_MS)
            self.total_bars += 1
            self.candidates, self.desired, self._candidate_key = (), {}, None
        needs_update = (
            (status.open_time_ms, status.symbols) != self._barstore_status_signature
            or status.updated_epoch > self._barstore_updated_epoch + 0.001)
        missing = any(not self.bar_store.bars.get(s)
                      or self.bar_store.bars[s][-1].open_time_ms < status.open_time_ms
                      for s in status.symbols)
        if needs_update or (missing and now >= self._barstore_retry_epoch):
            self.bar_store.update(status, now_epoch=now)
            self._barstore_retry_epoch = now + max(0.25, cfg.DB_SETTLE_SECONDS)
            self.warmup_bars = len({b.open_time_ms for rows in self.bar_store.bars.values() for b in rows})
            self.warmup_ready = self.warmup_bars >= cfg.QV_WINDOW + cfg.QV_MIN_PERIODS
            self._barstore_status_signature = (status.open_time_ms, status.symbols)
            self._barstore_updated_epoch = status.updated_epoch
        book_changed = self._advance_book(status, now=now) if self.warmup_ready else False
        outcomes_changed = self._resolve_ended_desired()
        book_changed = book_changed or outcomes_changed
        self.model_ready = self.warmup_ready and self.portfolio.last_bar == status.open_time_ms
        if not self.model_ready:
            changed = bool(self.candidates or self.desired or book_changed)
            for symbol in self.desired:
                self._add_plan_decision(Decision(symbol, 'withdraw', 'signal_model_not_ready'))
            self.candidates, self.desired, self._candidate_key = (), {}, None
            self.stats.candidates = self.stats.desired_orders = 0
            return changed
        key = (status.open_time_ms, status.symbols, self.portfolio.revision, self.bar_store.revision)
        if key == self._candidate_key:
            return book_changed
        self.candidates = build_candidates(
            self.bar_store.bars, reference_bar_open_ms=status.open_time_ms,
            symbols=status.symbols, notional=self._order_equity() / cfg.SIGNAL_SIZING_DIVISOR,
            qv20_fn=BarStore.qv20)
        self._candidate_key = key
        self.stats.candidates = len(self.candidates)
        return True

    def _portfolio_snapshot(self, *, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else float(now)
        return self.portfolio.snapshot(
            self.current_price.prices if self.current_price else {}, now=now,
            ready=self.model_ready and self.current_status is not None)

    def _publish_heartbeat(self, *, now: float, force: bool = False) -> None:
        self.publisher.publish(
            orders=tuple(self.desired.values()), bar_open_ms=self.stats.bar_open_ms,
            bar_stats=self.stats, portfolio=self._portfolio_snapshot(now=now),
            status_signature=self.current_status.signature if self.current_status else None,
            price_sequence=self.current_price.sequence if self.current_price else -1,
            now=now, force=force,
            candidate_templates=[asdict(c) for c in self.candidates
                                 if c.symbol not in self.portfolio.positions])

    def _evaluate_price(self, snapshot: PriceSnapshot | None, *, now: float | None = None) -> None:
        now = time.time() if now is None else float(now)
        self.total_evaluations += 1
        self.price_healthy = snapshot is not None
        self.nearest_candidates = ()
        if snapshot is None:
            for symbol in self.desired:
                self._add_plan_decision(Decision(symbol, 'withdraw', 'price_snapshot_unavailable'))
            self.desired = {}
            self.stats.desired_orders = self.stats.near_place_candidates = self.stats.touched_limit = 0
            self.stats.stale_price_symbols = len(self.candidates)
            self._publish_heartbeat(now=now)
            return
        prices = {}
        for candidate in self.candidates:
            quote = snapshot.prices.get(candidate.symbol)
            if quote is not None and -5 <= now - quote.exchange_time_ms / 1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS:
                prices[candidate.symbol] = SignalInput(
                    quote.price, quote.exchange_time_ms, long_gap(candidate.limit_price, quote.price))
        self.stats.stale_price_symbols = sum(c.symbol not in prices for c in self.candidates)
        self.stats.touched_limit = sum(q.gap < 0 for q in prices.values())
        self.stats.near_place_candidates = sum(math.isfinite(q.gap) and q.gap <= cfg.SIGNAL_DISCOVERY_GAP for q in prices.values())
        plan = select_orders(self.candidates, prices, held_symbols=self.portfolio.positions,
                             incumbent_symbols=self.desired,
                             place_gap=cfg.SIGNAL_DISCOVERY_GAP, cancel_gap=cfg.CANCEL_GAP)
        for decision in plan.decisions:
            self._add_plan_decision(decision)
        self.desired = {row.symbol: row for row in plan.desired}
        self.nearest_candidates = nearest_candidates(
            self.candidates, prices, held_symbols=self.portfolio.positions, desired_symbols=self.desired,
            place_gap=cfg.SIGNAL_DISCOVERY_GAP)
        self.stats.desired_orders = len(self.desired)
        self._publish_heartbeat(now=now)

    @staticmethod
    def _status_plan_key(status):
        return None if status is None else (status.open_time_ms, status.symbols)

    def _price_freshness_key(self, snapshot, *, now):
        if snapshot is None:
            return ()
        relevant = {c.symbol for c in self.candidates} | set(self.portfolio.positions)
        return tuple(sorted(s for s in relevant if s in snapshot.prices
                            and -5 <= now - snapshot.prices[s].exchange_time_ms / 1000
                            <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS))

    def _status_line(self) -> str:
        if self.current_status is None:
            from .input_diagnostics import status_reason
            reason = status_reason(self.status_reader._current, self.status_reader._json.error)
            return '等待有效的本地已完成3m数据；信号账本与未退出持仓保留' + f" [reason={reason}; path={cfg.STATUS_FILE.resolve()}]"
        now = time.time()
        age = now - self.price_reader.last_success_epoch
        left = (self.current_status.open_time_ms + cfg.ORDER_BAR_LIFETIME_MS) / 1000 - now
        model = 'ready' if self.model_ready else 'catching_up'
        price = f'{age:.1f}s' if math.isfinite(age) else '-'
        return (f'3m bar={self.current_status.open_time_ms}; warmup={self.warmup_bars}/{cfg.HISTORY_BARS}; '
                f'signal_book={model}; price_age={price}; active_left={left:.1f}s'
                + ('' if self.price_healthy else '; 价格不可用，暂不计划挂单'))

    def _render(self, *, force: bool = False) -> None:
        proximity_line = nearest_candidates_line(
            self.nearest_candidates, candidate_count=len(self.candidates),
            price_healthy=self.price_healthy, place_gap=cfg.SIGNAL_DISCOVERY_GAP)
        rendered = self.view.render(
            force=force, clock_utc=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            status_line=self._status_line(),
            status_key=(self.model_ready, self.price_healthy, self._status_plan_key(self.current_status)),
            bar_open_ms=self.stats.bar_open_ms, bar_available_ms=self.stats.bar_available_ms,
            reference_bar_open_ms=self.current_status.open_time_ms if self.current_status else 0,
            reference_bar_available_ms=self.current_status.open_time_ms + cfg.BAR_MS if self.current_status else 0,
            price_sequence=self.current_price.sequence if self.current_price else -1,
            market_revision=self.total_price_updates,
            portfolio=self._portfolio_snapshot(), desired=desired_map(self.desired),
            nearest_candidates_line=proximity_line,
            last_completed_positions=latest_completed_positions(self.portfolio.state['recent_trades']),
            bar_stats=self.stats.to_dict(), total_place=self.total_place, total_cancel=self.total_cancel,
            total_expire=self.total_expire, total_withdraw=self.total_withdraw,
            total_entry_confirmed=self.total_entry_confirmed,
            total_bars=self.total_bars, total_price_updates=self.total_price_updates,
            total_evaluations=self.total_evaluations,
            recent_decisions=[f'{d.action}:{d.symbol}:{d.reason}' for d in self.stats.decisions[-12:]])
        log_key = (self.stats.bar_open_ms, proximity_line)
        if rendered and log_key != self._last_logged_proximity:
            # Share the terminal throttle and ignore sequence-only changes.
            # Multiline candidates stay together under one timestamped log record.
            PROXIMITY_LOG.info('bar_open_ms=%s price_seq=%s %s', self.stats.bar_open_ms,
                               self.current_price.sequence if self.current_price else -1, proximity_line)
            self._last_logged_proximity = log_key

    def step(self) -> None:
        now = time.time()
        retired = self._retire_desired(now=now)
        previous_status = self._status_plan_key(self.current_status)
        status = self.status_reader.read()
        candidates_changed = retired
        if self._status_is_usable(status, now=now):
            self.current_status = status
            candidates_changed = self._refresh_bar(status, now=now) or retired
        else:
            self.current_status = None
            self.model_ready = False
            if self.candidates:
                self.candidates = ()
                self._candidate_key = None
                self.stats.candidates = 0
        status_changed = previous_status != self._status_plan_key(self.current_status)
        previous_healthy = self.price_healthy
        previous_missing = self.current_price is None
        new_snapshot = self.price_reader.read(only_new=True)
        if new_snapshot is not None:
            self.total_price_updates += 1
        self.current_price = self.price_reader.last_snapshot if self.price_reader.last_read_healthy else None
        self.price_healthy = self.current_price is not None
        freshness = self._price_freshness_key(self.current_price, now=now)
        freshness_changed = freshness != self._last_price_freshness_key
        self._last_price_freshness_key = freshness
        if (status_changed or candidates_changed or new_snapshot is not None
                or previous_healthy != self.price_healthy or (previous_missing and self.price_healthy)
                or freshness_changed or self.total_evaluations == 0):
            self._evaluate_price(self.current_price, now=now)
        else:
            self._publish_heartbeat(now=now)
        self._render(force=self.once)

    def run(self) -> int:
        LOG.info('start version=%s authority=signal book=%s desired=%s',
                 cfg.VERSION, self.portfolio.path, cfg.DESIRED_FILE)
        LOG.info('input paths 3m_status=%s price=%s mysql=%s:%s/%s history_bars=%s',
                 cfg.STATUS_FILE.resolve(), cfg.PRICE_FILE.resolve(),
                 cfg.MYSQL_HOST, cfg.MYSQL_PORT, cfg.MYSQL_DATABASE, cfg.HISTORY_BARS)
        poller = LocalChangePoller((cfg.STATUS_FILE, cfg.PRICE_FILE),
                                   maintenance_seconds=cfg.SIGNAL_MAINTENANCE_SECONDS)
        interval = min(cfg.STATUS_POLL_SECONDS, cfg.PRICE_POLL_SECONDS)
        LOG.info('local stat probes=%.3fs; safety/catchup maintenance=%.3fs; JSON decode on change only',
                 interval, cfg.SIGNAL_MAINTENANCE_SECONDS)
        while True:
            started = time.monotonic()
            try:
                if self.once or poller.due(now=started):
                    self.step()
                if self.once:
                    return 0
                time.sleep(probe_sleep(interval, started))
            except KeyboardInterrupt:
                LOG.info('stopped by user; signal book retained')
                return 0
            except Exception:
                LOG.exception('signal step failed; persisted book retained')
                if self.once:
                    return 1
                time.sleep(1.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mysql-host', default=None)
    parser.add_argument('--mysql-port', type=int, default=None)
    parser.add_argument('--mysql-user', default=None)
    parser.add_argument('--mysql-database', default=None)
    parser.add_argument('--mysql-password', default=None,
                        help='Override MYSQL_PASSWORD from the environment/private .env.')
    parser.add_argument('--status-file', type=Path, default=None,
                        help='Read the exact same completed-3m JSON path used by the collector.')
    parser.add_argument('--price-file', type=Path, default=None)
    parser.add_argument('--check-inputs', action='store_true',
                        help='Show read-only input paths and validity; do not connect MySQL or change state.')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args()
    for option, setting in (("mysql_host", "MYSQL_HOST"), ("mysql_port", "MYSQL_PORT"),
                            ("mysql_user", "MYSQL_USER"), ("mysql_database", "MYSQL_DATABASE"),
                            ("mysql_password", "MYSQL_PASSWORD")):
        value = getattr(args, option)
        if value is not None:
            setattr(cfg, setting, value)
    if args.status_file is not None:
        cfg.STATUS_FILE = args.status_file.expanduser().resolve()
    if args.price_file is not None:
        cfg.PRICE_FILE = args.price_file.expanduser().resolve()
    if args.check_inputs:
        from .input_diagnostics import check_inputs
        print(__import__('json').dumps(check_inputs(), ensure_ascii=False, indent=2))
        return 0
    cfg.STATE_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(cfg.LOG_FILE, encoding='utf-8')
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        handlers=[logging.StreamHandler(), file_handler])
    # The distance line is already printed inside the terminal frame. Send its
    # log copy only to signal.log, avoiding a duplicated console line.
    for file_logger in (PROXIMITY_LOG, PLAN_LOG):
        file_logger.propagate = False
        file_logger.addHandler(file_handler)
    # Reuse the collector's standard-library, OS-released single-writer lock.
    from .storage import SingleWriter
    with SingleWriter(cfg.STATE_DIR / 'signal.lock'):
        return LiveSignal(once=args.once).run()


if __name__ == '__main__':
    raise SystemExit(main())
