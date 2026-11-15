-- v9711 standalone MySQL schema. Run as an administrative MySQL user.
CREATE DATABASE IF NOT EXISTS binance_futures CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS binance_ticker_price CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE binance_futures;

CREATE TABLE IF NOT EXISTS klines_3m (
    symbol                  VARCHAR(32) NOT NULL,
    interval                VARCHAR(8) NOT NULL DEFAULT '3m',
    open_time_ms            BIGINT NOT NULL,
    open_time_utc           DATETIME(3) NOT NULL,
    close_time_ms           BIGINT NOT NULL,
    close_time_utc          DATETIME(3) NOT NULL,
    open_price              DECIMAL(38,16) NOT NULL,
    high_price              DECIMAL(38,16) NOT NULL,
    low_price               DECIMAL(38,16) NOT NULL,
    close_price             DECIMAL(38,16) NOT NULL,
    volume                  DECIMAL(38,8) NOT NULL,
    quote_volume            DECIMAL(38,8) NOT NULL,
    trade_count             BIGINT NOT NULL,
    first_trade_id         BIGINT NOT NULL,
    last_trade_id          BIGINT NOT NULL,
    taker_buy_volume       DECIMAL(38,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(38,8) NOT NULL,
    is_closed               TINYINT(1) NOT NULL DEFAULT 1,
    event_time_ms           BIGINT NOT NULL,
    created_at              DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at              DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (symbol, open_time_ms),
    KEY idx_klines_3m_open_time (open_time_ms),
    KEY idx_klines_3m_symbol_closed (symbol, is_closed, open_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS klines_15m (
    symbol                  VARCHAR(32) NOT NULL,
    open_time_ms            BIGINT NOT NULL,
    open_time_utc           DATETIME(3) NOT NULL,
    close_time_ms           BIGINT NOT NULL,
    close_time_utc          DATETIME(3) NOT NULL,
    open_price              DECIMAL(38,16) NOT NULL,
    high_price              DECIMAL(38,16) NOT NULL,
    low_price               DECIMAL(38,16) NOT NULL,
    close_price             DECIMAL(38,16) NOT NULL,
    volume                  DECIMAL(38,8) NOT NULL,
    quote_volume            DECIMAL(38,8) NOT NULL,
    trade_count             BIGINT NOT NULL,
    taker_buy_volume        DECIMAL(38,8) NOT NULL,
    taker_buy_quote_volume  DECIMAL(38,8) NOT NULL,
    is_closed               TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol, open_time_ms),
    KEY idx_klines_15m_open_time (open_time_ms),
    KEY idx_klines_15m_symbol_closed (symbol, is_closed, open_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS strategy_symbols (
    symbol VARCHAR(32) NOT NULL PRIMARY KEY,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

USE binance_ticker_price;
CREATE TABLE IF NOT EXISTS ticker_price_snapshots (
    captured_ms BIGINT NOT NULL PRIMARY KEY,
    symbol_count INT NOT NULL,
    prices JSON NOT NULL,
    KEY idx_ticker_captured (captured_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
