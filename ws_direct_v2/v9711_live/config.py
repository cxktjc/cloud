"""Shared configuration and frozen strategy constants for the live v9711 port.

This package separates signal from execution:

    signal/  - reads local 3m status/bar data and latest price snapshots,
               maintains signal-owned target positions/PnL independently,
               publishes target state to state/desired_orders.json
    execution/ - owns exact-order exchange actions, account stream identity
                reconciliation, timed exits and execution_feedback.json

The signal process has no websocket: it polls local JSON/MySQL only.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from .environment import load_environment, MYSQL_DEFAULT_PASSWORD

load_environment()

PACKAGE_DIR = Path(__file__).resolve().parent
# Paths are code-relative, never taken from a private .env or shell.
ROOT = PACKAGE_DIR.parent.resolve()
VAR_DIR = ROOT / 'var'
# Explicit paper mode retains the pre-existing paper ledger location.
PAPER_MODE = '--paper' in sys.argv[1:]


def env_float(name: str, default: float) -> float:
    value = str(os.environ.get(name, '') or '').strip()
    return float(value) if value else default


def env_int(name: str, default: int) -> int:
    value = str(os.environ.get(name, '') or '').strip()
    return int(value) if value else default


# Frozen v9711 strategy constants. These are an engineering port, not a new study.
VERSION = 'v9711_ws_direct_v2_503020'
SLOT_WEIGHTS = (0.5, 0.3, 0.2)
SIZING_POLICY = 'actual_position_count_503020_v2'
D = 0.025
K = 4
SLOTS = 3  # independent signal-model and ordinary trade position limits
SIGNAL_SIZING_DIVISOR = 3  # notional sizing; never a signal-count cap
BAR_MS = 180_000
EQUITY0 = 15.0
# Used only to finish/replay the explicitly recognized pre-upgrade strategy.
LEGACY_QV_THRESHOLD = 67522.82013208
TRADE_FEE_RATE = 0.0005
QV_WINDOW = 20
QV_MIN_PERIODS = 10
QV_QUANTILE = 0.90
QV_SMOOTH_BARS = 24
QV_REQUIRED_BARS = QV_WINDOW + QV_SMOOTH_BARS - 1

# Reuse parent deployment collectors by default; keep all new writers isolated.
SHARED_ROOT = ROOT.parent
SHARED_VAR_DIR = SHARED_ROOT / "var"

# Local data files.
STATUS_FILE = SHARED_VAR_DIR / 'klines_3m_status.json'
PRICE_FILE = VAR_DIR / 'binance_ticker_price' / 'latest.json'
SYMBOLS_FILE = ROOT / 'config' / 'symbols.txt'
STATE_DIR = VAR_DIR / ('paper_state' if PAPER_MODE else 'state')
DESIRED_FILE = STATE_DIR / 'desired_orders_ws.json'
SIGNAL_BOOK_FILENAME = 'signal_portfolio.json'
SIGNAL_CATCHUP_BARS = 40
EXECUTION_FEEDBACK_FILE = STATE_DIR / 'execution_feedback.json'
EXECUTION_LEDGER_FILE = STATE_DIR / 'execution_ledger.json'
EXECUTION_INTENT_FILE = STATE_DIR / 'execution_intents.json'
TRADE_LOG_FILE = STATE_DIR / 'trade.log'
TRADE_LOCK_FILE = STATE_DIR / 'trade.lock'
LOG_FILE = STATE_DIR / 'signal.log'
for _writer_path in (STATE_DIR, PRICE_FILE):
    if not _writer_path.expanduser().resolve().is_relative_to(ROOT):
        raise RuntimeError('WS-direct writer path outside new version: unset legacy state/price overrides; migrate state instead')

# MySQL connection defaults mirror the existing local research readers.
MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = env_int('MYSQL_PORT', 3306)
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', MYSQL_DEFAULT_PASSWORD)
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'binance_futures')
MYSQL_TABLE = 'klines_3m'

# 43 completed bars reconstruct all 24 historical quantiles; 96 also covers
# the bounded 40-bar catch-up. Old .env HISTORY_BARS=40 is upgraded in place.
HISTORY_BARS = max(96, env_int('V9711_LIVE_HISTORY_BARS', 96))
MYSQL_BATCH_SIZE = env_int('V9711_LIVE_MYSQL_BATCH_SIZE', 100)
# High-frequency stat-only probes, NOT collector/network request intervals.
STATUS_POLL_SECONDS = env_float('V9711_LIVE_STATUS_POLL_SECONDS', 0.02)
PRICE_POLL_SECONDS = env_float('V9711_LIVE_PRICE_POLL_SECONDS', 0.02)
SIGNAL_MAINTENANCE_SECONDS = env_float('V9711_LIVE_SIGNAL_MAINTENANCE_SECONDS', 0.25)
PRICE_MAX_AGE_SECONDS = env_float('V9711_LIVE_PRICE_MAX_AGE_SECONDS', 75.0)
# A transient collector error may still leave the prior successful price set
# usable. This grace applies to `last_success_epoch`, not to stale per-symbol
# exchange trades, which have their own stricter check below.
PRICE_ERROR_GRACE_SECONDS = env_float('V9711_LIVE_PRICE_ERROR_GRACE_SECONDS', 60.0)
# A per-symbol exchange trade may be old while the REST snapshot itself is fresh.
# Do not use a price whose own exchange_time is this stale for place decisions.
PRICE_EXCHANGE_MAX_AGE_SECONDS = env_float('V9711_LIVE_PRICE_EXCHANGE_MAX_AGE_SECONDS', 60.0)
STATUS_GRACE_SECONDS = env_float('V9711_LIVE_STATUS_GRACE_SECONDS', 210.0)
DB_SETTLE_SECONDS = env_float('V9711_LIVE_DB_SETTLE_SECONDS', 4.0)
# A status bar is only a frozen planning reference. It stops being an active
# order interval at open_time + 2*3m (its own close plus one next-bar open).
ORDER_BAR_LIFETIME_MS = 2 * BAR_MS

# Current-bar pending order policy. Signed gap includes all below-limit prices.
# Signal publishes eligible candidates up to +2.15%; trade admits the
# first five at +2.15%, orders 6-8 at +1.95%, and full-book replacements at +1.75%.
# During a held position's final pre-exit 3m bar, pending admission ignores position cap.
# Fixed v2 policy: legacy gap overrides must not silently retain old tiers.
PLACE_GAP = 0.0175
# Signal publishes the discovery envelope; trade applies count-based tiers.
BOOTSTRAP_PLACE_GAP = 0.0215
FILL_PLACE_GAP = 0.0195
BOOTSTRAP_PENDING_ORDERS = 5
SIGNAL_DISCOVERY_GAP = BOOTSTRAP_PLACE_GAP
if not (0 <= PLACE_GAP <= FILL_PLACE_GAP <= BOOTSTRAP_PLACE_GAP < 1):
    raise ValueError('Require strict <= fill <= bootstrap pending-entry gaps')
MAX_PENDING_ORDERS = env_int('V9711_LIVE_MAX_PENDING_ORDERS', 8)
SIGNAL_FOLLOW_MAX_PENDING_ORDERS = 2
SIGNAL_FOLLOW_POSITION_STOP = 6
SWITCH_TOLERANCE = env_float('V9711_LIVE_SWITCH_TOLERANCE', 0.005)
# Kept as numeric compatibility/diagnostic constants for older readers.
# Current eight-slot policy does not use an upward price gap to auto-cancel.
CANCEL_GAP = env_float('V9711_LIVE_CANCEL_GAP', 0.02)
PENDING_ORDER_POLICY = 'tiered_5_8_rotation_first_v5'
if not (0.0 <= PLACE_GAP < 1.0 and 0.0 <= SWITCH_TOLERANCE < 1.0):
    raise ValueError('Require finite place/switch gaps in [0, 1)')
if not 1 <= MAX_PENDING_ORDERS <= 8:
    raise ValueError('Pending order limit must be between 1 and 8')

EXIT_BARS = env_int('V9711_LIVE_EXIT_BARS', 4)
REOPEN_PROTECTION_SECONDS = env_float('V9711_LIVE_REOPEN_PROTECTION_SECONDS', 60.0)
TRADE_POLL_SECONDS = env_float('V9711_LIVE_TRADE_POLL_SECONDS', 0.02)
TRADE_MAINTENANCE_SECONDS = env_float('V9711_LIVE_TRADE_MAINTENANCE_SECONDS', 0.5)
ORDER_RECONCILE_SECONDS = env_float('V9711_LIVE_ORDER_RECONCILE_SECONDS', 2.0)
ACCOUNT_STREAM_DIR = SHARED_VAR_DIR / 'binance_account_stream'
ACCOUNT_STREAM_MAX_AGE_SECONDS = env_float('V9711_LIVE_ACCOUNT_STREAM_MAX_AGE_SECONDS', 15.0)
ACCOUNT_ID = os.environ.get('V9711_LIVE_ACCOUNT_ID', 'default') or 'default'
EXCHANGE_INFO_CACHE_FILE = VAR_DIR / 'exchange_info_cache.json'
ORDER_REST_FALLBACK_SECONDS = env_float('V9711_LIVE_ORDER_REST_FALLBACK_SECONDS', 300.0)
# Fast lane is only for an unacknowledged submission still actionable now.
# Stable/acknowledged orders keep ORDER_REST_FALLBACK_SECONDS above.
ORDER_UNKNOWN_FALLBACK_SECONDS = env_float('V9711_LIVE_UNKNOWN_QUERY_AFTER_SECONDS', 2.0)
ORDER_UNKNOWN_QUERY_INTERVAL_SECONDS = env_float('V9711_LIVE_UNKNOWN_QUERY_INTERVAL_SECONDS', 2.0)
ORDER_UNKNOWN_MAX_RETRY_SECONDS = env_float('V9711_LIVE_UNKNOWN_QUERY_MAX_RETRY_SECONDS', 10.0)
ORDER_UNKNOWN_SETTLE_SECONDS = env_float('V9711_LIVE_UNKNOWN_SETTLE_SECONDS', 5.0)
ENTRY_RECOVERY_MAX_RETRIES = env_int('V9711_LIVE_ENTRY_RECOVERY_MAX_RETRIES', 2)
# No idle reconnect: trading sockets use Ping/Pong and background recovery.
ORDER_WS_RECONNECT_RETRY_SECONDS = env_float('V9711_LIVE_WS_RECONNECT_RETRY_SECONDS', 2.0)
if not (0.5 <= ORDER_UNKNOWN_FALLBACK_SECONDS <= 30.0
        and 0.5 <= ORDER_UNKNOWN_QUERY_INTERVAL_SECONDS <= ORDER_UNKNOWN_MAX_RETRY_SECONDS <= 60.0
        and max(ORDER_UNKNOWN_FALLBACK_SECONDS, ORDER_UNKNOWN_QUERY_INTERVAL_SECONDS) <= ORDER_UNKNOWN_SETTLE_SECONDS <= 60.0
        and 0 <= ENTRY_RECOVERY_MAX_RETRIES <= 5
        and 1.0 <= ORDER_WS_RECONNECT_RETRY_SECONDS <= 30.0):
    raise ValueError('Invalid fast unknown-submit recovery / WS reconnect policy')
ORDER_RECONCILE_MIN_INTERVAL_SECONDS = env_float(
    'V9711_LIVE_ORDER_RECONCILE_MIN_INTERVAL_SECONDS', 1.0)
ORDER_CLIENT_PREFIX = 'v9712l'
TRADE_ENABLED = os.environ.get('V9711_LIVE_TRADE_ENABLED', '0').strip().lower() in {
    '1', 'true', 'yes', 'on'}
DRY_RUN = os.environ.get('V9711_LIVE_DRY_RUN', '1').strip().lower() in {
    '1', 'true', 'yes', 'on'}

# Snapshot TTL must materially exceed price refresh. No content change means no
# file rewrite; heartbeat refresh only extends expiry to keep the executor safe.
DESIRED_TTL_SECONDS = env_float('V9711_LIVE_DESIRED_TTL_SECONDS', 30.0)
DESIRED_HEARTBEAT_SECONDS = env_float('V9711_LIVE_DESIRED_HEARTBEAT_SECONDS', 10.0)
EXECUTION_FEEDBACK_MAX_AGE_SECONDS = env_float('V9711_LIVE_FEEDBACK_MAX_AGE_SECONDS', 60.0)


@dataclass(frozen=True)
class ViewConfig:
    """Terminal rendering controls: changed content only; no timed redraw."""
    min_refresh_seconds: float = env_float('V9711_LIVE_MIN_REFRESH_SECONDS', 0.1)
    # Legacy argument retained for compatibility; file heartbeat is independent.
    heartbeat_seconds: float = env_float('V9711_LIVE_HEARTBEAT_SECONDS', 60.0)
    max_orders_display: int = env_int('V9711_LIVE_MAX_ORDERS_DISPLAY', 12)


@dataclass(frozen=True)
class SignalViewConfig(ViewConfig):
    """Signal-only display throttle; execution and plan publication are unchanged."""
    min_refresh_seconds: float = env_float(
        'V9711_LIVE_SIGNAL_MIN_REFRESH_SECONDS', 3.0)


@dataclass(frozen=True)
class TradeViewConfig(ViewConfig):
    """Execution display only; never throttles signal polling or order I/O."""
    min_refresh_seconds: float = env_float(
        'V9711_LIVE_TRADE_MIN_REFRESH_SECONDS', 3.0)

WS_DIRECT_MODE = "ws_direct_v2"
WS_MIRROR_SECONDS = env_float("V9711_WS_MIRROR_SECONDS", 0.1)
WS_MAX_QUEUE_AGE_SECONDS = env_float("V9711_WS_MAX_QUEUE_AGE_SECONDS", 0.5)
WS_QUEUE_SIZE = env_int("V9711_WS_QUEUE_SIZE", 8192)
